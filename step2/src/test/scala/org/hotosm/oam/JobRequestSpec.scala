package org.hotosm.oam

import spray.json._

import org.scalatest._

class JobRequestSpec extends FunSpec with Matchers {
  val testJson = """
{
  "jobId" : "nepal_1",
  "target" : "s3://hotosm-oam-tiles/nepal_1",
  "input" : [
    { 
      "cellSize" : {
        "width" : 1234.4321,
        "height" : 6789.9876
      },
      "extent" : {
        "xmin" : -123.4324,
        "ymin" : -123.4324,
        "xmax" : 123.4324,
        "ymax" : 123.4324
      },
      "images" : "s3://workspace-oam-hotosm-org/image1/chunked"
    },
    { 
      "cellSize" : {
        "width" : 234.4321,
        "height" : 789.9876
      },
      "extent" : {
        "xmin" : -123.4324,
        "ymin" : -123.4324,
        "xmax" : 123.4324,
        "ymax" : 123.4324
      },
      "images" : "s3://workspace-oam-hotosm-org/image2/chunked"
    }
  ]
}
"""

  describe("JobRequest parsing") {
    it("should parse the example json") {
      val jr = testJson.parseJson.convertTo[JobRequest]
      jr.inputImages.size should be (2)
      jr.local should be (false)
    }

    it("should pick up the local property for testing") {
      val jr = 
        """{ "jobId" : "nepal_1", "target" : "s3://hotosm-oam-tiles/nepal_1", "local" : 1, "input" : [] }""".parseJson.convertTo[JobRequest]
      jr.local should be (true)
    }
  }
}
