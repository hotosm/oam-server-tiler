package org.hotosm.oam

import spray.json._
import geotrellis.raster._

import org.scalatest._

class JobRequestSpec extends FunSpec with Matchers {
  val testJson = """
{
  "jobId": "test-job",
  "target": "/Users/rob/proj/oam/data/results",
  "tileSize": 1024,
  "input": [
    {
      "gridBounds": {
        "rowMax": 109878,
        "colMax": 193078,
        "colMin": 193040,
        "rowMin": 109836
      },
      "tiles": "/Users/rob/proj/oam/data/workspace/356f564e3a0dc9d15553c17cf4583f21-6",
      "zoom": 18,
      "extent": {
        "xmin": 85.10125004428353,
        "ymin": 27.92802942514566,
        "ymax": 27.97940067918027,
        "xmax": 85.1529804360412
      },
      "sourceUri": "s3://bucket/source.tif"
    },
    {
      "gridBounds": {
        "rowMax": 1736,
        "colMax": 3013,
        "colMin": 2986,
        "rowMin": 1709
      },
      "tiles": "/Users/rob/proj/oam/data/workspace/LC81420412015111LGN00_bands_432",
      "zoom": 12,
      "extent": {
        "xmin": 82.52879450382254,
        "ymin": 26.359556467334755,
        "ymax": 28.486769014205997,
        "xmax": 84.86307565466431
      },
      "sourceUri": "s3://bucket/source2.tif"
    }
  ]
}
"""

  describe("JobRequest parsing") {
    it("should parse the example json") {
      val jr = testJson.parseJson.convertTo[JobRequest]
      jr.inputImageDefinitions.size should be (2)
      jr.inputImageDefinitions.map(_.gridBounds).toSet should be (
        Set(GridBounds(2986, 1709, 3013, 1736), GridBounds(193040, 109836, 193078, 109878))
      )

    }
  }
}
