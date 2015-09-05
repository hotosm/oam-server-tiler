package org.hotosm.oam

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark._
import org.apache.spark.rdd._

/** Operation which takes a set of images and cuts them into tiles according to the layout scheme */
object CutTiles {
  def apply(images: RDD[MultiBandTile], tileLayout: TileLayout): RDD[(SpatialKey, MultiBandTile)] = ???
}
