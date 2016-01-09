package org.hotosm.oam

import org.apache.spark.rdd._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.mosaic._
import geotrellis.vector._
import geotrellis.spark._

import spire.syntax.cfor._

object ZoomUp {
  private val threshold = 0.0000001

  def mergeImage(nextTile: Array[MutableArrayTile], thisTile: MultiBandTile, nextExtent: Extent, thisExtent: Extent): Unit =
    nextExtent & thisExtent match {
      case Some(intersection) =>
        val re = RasterExtent(thisExtent, TileMath.TILE_DIM, TileMath.TILE_DIM)
        val dx = re.cellwidth / 2
        val dy = re.cellheight / 2
        val colMin =
          if(thisExtent.xmin - nextExtent.xmin > threshold) { TileMath.TILE_DIM / 2 } else { 0 }

        val colLim =
          if(colMin == 0) { TileMath.TILE_DIM / 2 } else { TileMath.TILE_DIM }

        val rowMin =
          if(nextExtent.ymax - thisExtent.ymax > threshold) { TileMath.TILE_DIM / 2 } else { 0 }

        val rowLim =
          if(rowMin == 0) { TileMath.TILE_DIM / 2 } else { TileMath.TILE_DIM }

        cfor(0)(_ < TileMath.BAND_COUNT, _ + 1) { b =>
          val targetBand = nextTile(b)
          val resampleTile =
            thisTile.band(b).convert(TypeShort)
              .map { z =>
              if(isNoData(z)) { 128 }
              else if(z == 0) { NODATA }
              else { z.toByte & 0xFF }
            }
          val resampler = Resample(Bilinear, resampleTile, thisExtent)
          cfor(rowMin)(_ < rowLim, _ + 1) { row =>
            cfor(colMin)( _ < colLim, _ + 1) { col =>
              val (x, y) = re.gridToMap((col - colMin) * 2, (row - rowMin) * 2)
              targetBand.set(col, row, resampler.resample(x + dx, y + dy) & 0xFF)
            }
          }
        }

      case None =>
    }

  def apply(images: RDD[(SpatialKey, OrderedImage)], thisZoom: Int, partitionerFactory: TilePartitionerFactory): RDD[(SpatialKey, OrderedImage)] = {
    println(s"ZOOM UP $thisZoom!")
    val tileCount = 0

    images
      .map { case (key, image) =>
        val newKey = SpatialKey(key.col / 2, key.row / 2)
        (newKey, (key, image))
      }
      .groupByKey(partitionerFactory.mergedAt(thisZoom - 1))
      .mapPartitions({ partition =>
        partition.map { case (nextKey, seqTiles) =>
          val nextExtent = TileMath.getExtent(thisZoom - 1, nextKey.col, nextKey.row)

          val arr = Array.ofDim[Byte](TileMath.TILE_DIM * TileMath.TILE_DIM)
          val nextImage: Array[MutableArrayTile] =
            Array(
              ByteArrayTile(arr.clone, TileMath.TILE_DIM, TileMath.TILE_DIM),
              ByteArrayTile(arr.clone, TileMath.TILE_DIM, TileMath.TILE_DIM),
              ByteArrayTile(arr.clone, TileMath.TILE_DIM, TileMath.TILE_DIM)
            )

          var nextOrder: Tile =
            IntConstantTile(NODATA, TileMath.TILE_DIM, TileMath.TILE_DIM)

          for((thisKey, thisImage) <- seqTiles) {
            val thisExtent = TileMath.getExtent(thisZoom, thisKey.col, thisKey.row)

            ZoomUp.mergeImage(nextImage, thisImage.image, nextExtent, thisExtent)
            nextOrder = nextOrder.merge(nextExtent, thisExtent, thisImage.order)
          }

          (nextKey, OrderedImage(ArrayMultiBandTile(nextImage), nextOrder))
        }
      }, preservesPartitioning = true)
  }

  object Implicits {
    implicit class ZoomWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
      def zoomUp(thisZoom: Int, partitionerFactory: TilePartitionerFactory): RDD[(SpatialKey, OrderedImage)] =
        ZoomUp.apply(images, thisZoom, partitionerFactory)
    }
  }
}
