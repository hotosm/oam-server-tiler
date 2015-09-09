package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.mosaic._

import spire.syntax.cfor._

case class OrderedImage(image: MultiBandTile, order: Tile) {
  def merge(other: OrderedImage): OrderedImage = {
    val bandCount = TileMath.BAND_COUNT
    val bands = Array.ofDim[MutableArrayTile](bandCount)
    val newOrder = ArrayTile.empty(order.cellType, order.cols, order.rows)

    cfor(0)(_ < bandCount, _ + 1) { b =>
      bands(b) = ByteArrayTile(Array.ofDim[Byte](image.cols * image.rows), image.cols, image.rows)
    }

    cfor(0)(_ < image.rows, _ + 1) { row =>
      cfor(0)(_ < image.cols, _ + 1) { col =>
        val o1 = order.get(col, row)
        val o2 = other.order.get(col, row)

        var b = 0
        while(b < bandCount) {
          val v1 = image.band(b).get(col, row)
          val v2 = other.image.band(b).get(col, row)

          if(v1 == 0 || (o2 > o1 && v2 != 0)) {
            bands(b).set(col, row, v2)
            newOrder.set(col, row, o2)
          } else if(v2 == 0 || (o1 > o2 && v1 != 0)) {
            bands(b).set(col, row, v1)
            newOrder.set(col, row, o1)
          }

          b += 1
        }
      }
    }

    OrderedImage(ArrayMultiBandTile(bands), newOrder)
  }
}

object OrderedImage {
  implicit def orderedToTile(oi: OrderedImage): MultiBandTile =
    oi.image

  def merge(oi1: OrderedImage, oi2: OrderedImage): OrderedImage =
    oi1.merge(oi2)
}
