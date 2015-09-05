package org.hotosm.oam

import sys.process._

/** Shells out to GDAL */
object GDAL {
  def reproject(source: String, target: String): String = {
    val options = Seq(
      "-q", "-overwrite",
      "-wo", "NUM_THREADS=ALL_CPUS",
      "-multi",
      "-co", "tiled=yes",
      "-co", "compress=lzw",
      "-co", "predictor=2",
      "-r", "bilinear").mkString(" ")


    val reprojectCommand =
      s"gdalwarp -t_srs EPSG:3857 $options $source $target"
    println(reprojectCommand)
    val result =  reprojectCommand !

    if(result != 0) sys.error(s"Failed: $reprojectCommand")
    target
  }
}
