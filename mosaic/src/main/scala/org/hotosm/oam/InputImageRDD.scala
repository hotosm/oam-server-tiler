package org.hotosm.oam

import org.apache.spark.rdd.RDD
import geotrellis.raster._
import geotrellis.spark.SpatialKey

import spire.syntax.cfor._

object InputImageRDD {
  def split(key: SpatialKey, image: MultiBandTile, tilesAcross: Int): Array[(SpatialKey, MultiBandTile)] = {
    val newKeyColMin = key.col * tilesAcross
    val newKeyRowMin = key.row * tilesAcross
    val tiles = Array.ofDim[(SpatialKey, MultiBandTile)](tilesAcross * tilesAcross)
    val bandCount = image.bandCount
    var i = 0
    cfor(0)(_ < tilesAcross, _ + 1) { layoutRow =>
      cfor(0)(_ < tilesAcross, _ + 1) { layoutCol =>
        val firstCol = layoutCol * TileMath.TILE_DIM
        val lastCol = firstCol + TileMath.TILE_DIM - 1
        val firstRow = layoutRow * TileMath.TILE_DIM
        val lastRow = firstRow + TileMath.TILE_DIM - 1

        tiles(i) = {
          val gb = GridBounds(firstCol, firstRow, lastCol, lastRow)
          val bands = Array.ofDim[Tile](bandCount)
          cfor(0)(_ < bandCount, _ + 1) { b =>
            bands(b) = CroppedTile(image.band(b), gb).toArrayTile
          }

          val newKey = SpatialKey(newKeyColMin + layoutCol, newKeyRowMin + layoutRow)

          (newKey, ArrayMultiBandTile(bands))
        }

        i += 1
      }
    }

    return tiles
  }
}

case class InputImageRDD(priority: Int, zoom: Int, gridBounds: GridBounds, images: RDD[(SpatialKey, MultiBandTile)]) {
  def orderedImages: RDD[(SpatialKey, OrderedImage)] =
    images
      .mapValues { image => 
        OrderedImage(image, IntConstantTile(priority, image.cols, image.rows))
      }
}
