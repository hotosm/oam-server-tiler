package org.hotosm.oam

import spray.json._
import geotrellis.raster._

import org.scalatest._

class TileCountsSpec extends FunSpec with Matchers {
  val zoom5GB = GridBounds(50 * 2, 53 * 2, 60 * 2, 63 * 2)
  val zoom4GB = GridBounds(50, 53, 60, 73)

  val bounds = Seq(
    (5, zoom5GB),
    (4, zoom4GB)
  )

  describe("TileCounts") {
    it("should give correct results for sample set") {
      val tileCounts = TileCounts(bounds)
      val z4size5GB = GridBounds(zoom5GB.colMin / 2, zoom5GB.rowMin / 2, zoom5GB.colMax / 2, zoom5GB.rowMax / 2).size
      val z4sizeMerged4GB = GridBounds(50, 64, 60, 73).size

      tileCounts.mergedAt(5) should be (zoom5GB.size)
      tileCounts.unmergedAt(4) should be (z4size5GB + zoom4GB.size)
      tileCounts.mergedAt(4) should be (z4size5GB + (z4sizeMerged4GB))
    }
  }
}
