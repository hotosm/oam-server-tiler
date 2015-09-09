package org.hotosm.oam

import org.hotosm.oam.io._

import org.apache.spark._
import org.apache.spark.rdd._

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.mosaic._
import geotrellis.raster.io.geotiff._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._

import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.Files

import spray.json._
import spire.syntax.cfor._

object Tiler {
  def getUpperLeft(zoom: Int, col: Int, row: Int) = {
    val n = math.pow(2, zoom)
    val long = ((col / n) * 360.0) - 180.0
    val lat = math.toDegrees(math.atan(math.sinh(math.Pi * (1 - 2 * row / n))))
    (long, lat)
  }

  def getExtent(zoom: Int, col: Int, row: Int) = {
    val (xmin, ymax) = getUpperLeft(zoom, col, row)
    val (xmax, ymin) = getUpperLeft(zoom, col + 1, row + 1)
    Extent(xmin, ymin, xmax, ymax)
  }

  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
//        .setMaster("local[8]")
        .setAppName("HOT OSM Open Aerial Map Tiler")

    new SparkContext(conf)
  }

  implicit class SaveWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def save(zoom: Int, sink: (Int, SpatialKey, MultiBandTile) => Unit): RDD[(SpatialKey, OrderedImage)] =
      images.mapPartitions({ partition =>
        for(e @ (key, OrderedImage(tile, _)) <- partition) yield {
          sink(zoom, key, tile)
          e
        }
      }, preservesPartitioning = true)
    // def save(zoom: Int, sink: (Int, SpatialKey, MultiBandTile) => Unit): Unit =
    //   images.foreach { case (key, OrderedImage(tile, _)) =>
    //     sink(zoom, key, tile)
    //   }
  }

  object ZoomWrapper {
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
  }

  implicit class ZoomWrapper(images: RDD[(SpatialKey, OrderedImage)]) {

    def zoomUp(thisZoom: Int, partitionerFactory: TilePartitionerFactory): RDD[(SpatialKey, OrderedImage)] = {
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
            val nextExtent = getExtent(thisZoom - 1, nextKey.col, nextKey.row)

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
              val thisExtent = getExtent(thisZoom, thisKey.col, thisKey.row)
              if(thisExtent.width  < 0.001) {
                sys.error(s"THIS ONE $thisExtent $thisZoom $thisKey")
              }

              ZoomWrapper.mergeImage(nextImage, thisImage.image, nextExtent, thisExtent)
              nextOrder = nextOrder.merge(nextExtent, thisExtent, thisImage.order)
            }

            (nextKey, OrderedImage(ArrayMultiBandTile(nextImage), nextOrder))
          }
        }, preservesPartitioning = true)
    }
  }

  implicit class ProcessBetweenWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def processBetween(z1: Int, z2: Int, partitionerFactory: TilePartitionerFactory, sink: (Int, SpatialKey, MultiBandTile) => Unit): RDD[(SpatialKey, OrderedImage)] = {
      val merged =
        images
          .reduceByKey(OrderedImage.merge)
          .save(z1, sink)

//      merged.save(z1, sink)
        
      val zoomed =
        merged.zoomUp(z1, partitionerFactory)

      if(z1 - 1 != z2) {
        zoomed
          .processBetween(z1 - 1, z2, partitionerFactory, sink)
      } else {
        zoomed
      }
    }
  }

  def apply(inputImages: Seq[(Int, Int, GridBounds, RDD[(SpatialKey, MultiBandTile)])])
           (sink: (Int, SpatialKey, MultiBandTile) => Unit)
           (implicit sc: SparkContext): Unit = {
    val tileCounts =
      TileCounts(inputImages.map { case (_, zoom, gbs, _) => (zoom, gbs) })
    val partitionerFactory = TilePartitionerFactory(20, tileCounts)

    val zoomsToTiles: Map[Int, RDD[(SpatialKey, OrderedImage)]] =
      inputImages
        .map { case (priority, zoom, _, images) =>
          (zoom,
            images
              .mapValues { image => OrderedImage(image, IntConstantTile(priority, image.cols, image.rows)) }
          )
        }
        .foldLeft(Map[Int, RDD[(SpatialKey, OrderedImage)]]()) { (acc, tup) =>
          val (zoom, theseTiles) = tup
          val tiles =
            acc.get(zoom) match {
              case Some(t) => t.union(theseTiles)
              case None => theseTiles
            }
          acc + ((zoom, tiles))
        }

    val sortedZooms =
      (zoomsToTiles.keys.toSeq :+ 1)
        .sortBy(-_)
        .toVector
    val zoomGroups =
      sortedZooms
        .sliding(2)

    val firstZoom = sortedZooms.head

    zoomGroups.foldLeft(None: Option[RDD[(SpatialKey, OrderedImage)]]) { (prev, zooms) =>
      val Seq(z1, z2) = zooms
      val tiles =
        prev match {
          case Some(t) => t.union(zoomsToTiles(z1))
          case None => zoomsToTiles(z1)
        }

      Some(
        tiles
          .partitionBy(partitionerFactory.unmergedAt(z1))
          .processBetween(z1, z2, partitionerFactory, sink)
      )
    } match {
      case Some(rdd) => 
        // Action to kick off process
        rdd.save(1, sink).count
      case None =>
    }
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 1, "Arguement error: must only be one argument, that is the URI to the request JSON")

    val client = AWSClient.default

    val jobRequest = {
      val uri = args(0)
      new java.net.URI(uri).getScheme match {
        case null =>
          scala.io.Source.fromFile(uri).getLines.mkString.parseJson.convertTo[JobRequest]
        case _ =>
          client.readTextFile(uri).parseJson.convertTo[JobRequest]
      }
    }

    implicit val sc = getSparkContext()

    try {
      val inputImages: Seq[(Int, Int, GridBounds, RDD[(SpatialKey, MultiBandTile)])] =
        jobRequest.inputImages
          .map { ii =>
           (ii.priority, ii.zoom, ii.gridBounds, TileServiceReader[MultiBandTile](ii.imagesFolder).read(ii.zoom))
          }

      val sink = 
        new java.net.URI(jobRequest.target).getScheme match {
          case null => new LocalSink(jobRequest.target)
          case _ => new S3Sink(client, jobRequest.target)
        }

      apply(inputImages)(sink)

    } finally {
      sc.stop
    }
  }

  // import org.hotosm.oam.io._
  // def main(args: Array[String]): Unit = {
  //   implicit val sc = getSparkContext()

  //   try {
  //     val tileReader = TileServiceReader[MultiBandTile]("/Users/rob/proj/oam/data/workspace/LC81420412015111LGN00_bands_432")
  //     val tiles = tileReader.read(12)
  //     println(s"${tiles.count}")
  //   } finally {
  //     sc.stop
  //   }
  // }
}
