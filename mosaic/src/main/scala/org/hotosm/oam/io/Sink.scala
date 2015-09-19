package org.hotosm.oam.io

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
    val png = convertToPng(tile)
    val bytes = png.bytes
    val len = bytes.length
    val z = 0.toByte
    var break = false
    var i = 0
    while(!break && i < len) { 
      if(bytes(i) != z) {
        png.write(path.getAbsolutePath)
        break = true
      }
    }
  }
}

class S3Sink(bucket: String, s3Key: String) extends Sink {
  val client = S3Client.default
  def apply(zoom: Int, spatialKey: SpatialKey, tile: MultiBandTile): Unit = {
    val tileKey = s"$s3Key/$zoom/${spatialKey.col}/${spatialKey.row}.png"
    val png = convertToPng(tile)
    val bytes = png.bytes
    val len = bytes.length
    val z = 0.toByte
    var break = false
    var i = 0
    while(!break && i < len) { 
      if(bytes(i) != z) {
        client.putBytes(bucket, tileKey, bytes)
        break = true
      }
    }
  }
}
