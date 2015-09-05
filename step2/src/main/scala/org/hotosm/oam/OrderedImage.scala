package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.mosaic._

case class OrderedImage(image: MultiBandTile, order: Int) {
  def merge(other: OrderedImage): OrderedImage = {
    if(order > other.order)
      OrderedImage(image.merge(other.image), order)
    else
      OrderedImage(other.image.merge(image), other.order)
  }
}

object OrderedImage {
  implicit def orderedToTile(oi: OrderedImage): MultiBandTile =
    oi.image

  def merge(oi1: OrderedImage, oi2: OrderedImage): OrderedImage =
    oi1.merge(oi2)
}
