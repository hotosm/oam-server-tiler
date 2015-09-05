package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.spark._

/** Function that saves images */
trait Sink extends Function3[Int, SpatialKey, MultiBandTile, Unit] with Serializable {
  def convertToPng(tile: MultiBandTile): Png = {
    tile
      .convert(TypeInt).combine(0, 1, 2) { (r, g, b) =>
        val cr = if(isNoData(r)) 128 else r.toByte & 0xFF
        val cg = if(isNoData(g)) 128 else g.toByte & 0xFF
        val cb = if(isNoData(b)) 128 else b.toByte & 0xFF
        if(cr == 0 && cg == 0 && cb == 0)
          0
        else
          (cr << 24) | (cg << 16) | (cb << 8) | 0xFF
      }
      .renderPng
  }
}

class LocalSink(target: String) extends Sink {
  def apply(zoom: Int, key: SpatialKey, tile: MultiBandTile): Unit = {
    val path = new java.io.File(target, s"$zoom/${key.col}/${key.row}.png")
    // Ensure directory exists
    path.getParentFile.mkdirs()
    convertToPng(tile).write(path.getAbsolutePath)
  }
}

class S3Sink(client: AWSClient, target: String) extends Sink {
  def apply(zoom: Int, key: SpatialKey, tile: MultiBandTile): Unit = ???
}
