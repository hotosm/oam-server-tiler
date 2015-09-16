package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.io.geotiff._

package object io {
  implicit object TileByteReader extends ByteReader[Tile] {
    def read(bytes: Array[Byte]): Tile =
      SingleBandGeoTiff(bytes).tile
  }

  implicit object MultiBandTileByteReader extends ByteReader[MultiBandTile] {
    def read(bytes: Array[Byte]): MultiBandTile =
      MultiBandGeoTiff(bytes).tile
  }
}
