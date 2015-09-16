package org.hotosm.oam

import geotrellis.vector._

object TileMath {
  val TILE_DIM = 256
  val BAND_COUNT = 3

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
}
