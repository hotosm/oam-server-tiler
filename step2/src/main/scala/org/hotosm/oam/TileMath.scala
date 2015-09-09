package org.hotosm.oam

import geotrellis.vector._
import geotrellis.spark.tiling._

object TileMath {
  val TILE_DIM = 256
  val BAND_COUNT = 3
  val LAYOUT_SCHEME = ZoomedLayoutScheme(TILE_DIM)
  val WORLD_EXTENT = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244)
  val WORLD_WIDTH = 20026376.39 * 2

  def zoomFor(resolution: Double): Int = {
    def _zoomFor(z: Int): Int = {
      val r2 = WORLD_WIDTH / (math.pow(2, z + 1) * 256)
      val r1 = WORLD_WIDTH / (math.pow(2, z) * 256)
      if(r2 < resolution) {
        val dRes = r1 - resolution
        val dZoom = r1 - r2
        if(dRes * 3 < dZoom) {
          z
        } else {
          z + 1
        }
      } else {
        _zoomFor(z + 1)
      }
    }

    return _zoomFor(2)
  }

  def mapTransformFor(zoom: Int): MapKeyTransform = {
    val LayoutLevel(_, tileLayout) = LAYOUT_SCHEME.levelFor(zoom)
    MapKeyTransform(WORLD_EXTENT, tileLayout.layoutCols, tileLayout.layoutRows)
  }
}
