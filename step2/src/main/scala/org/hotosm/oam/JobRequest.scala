package org.hotosm.oam

import geotrellis.vector._
import geotrellis.vector.io.json._
import geotrellis.raster._
import geotrellis.raster.io.json._
import geotrellis.spark.tiling._
import spray.json._

case class InputImage(zoom: Int, gridBounds: GridBounds, imagesFolder: String, priority: Int = 0) {
  def withPriority(p: Int) =
    InputImage(zoom, gridBounds, imagesFolder, p)
}

object InputImage {
  implicit object InputImageReader extends RootJsonReader[InputImage] {
    def read(v: JsValue): InputImage =
      v.asJsObject.getFields("zoom", "gridBounds", "tiles") match {
        case Seq(JsNumber(zoom), gridBounds, JsString(imagesFolder)) =>
          InputImage(
            zoom.toInt,
            gridBounds.convertTo[GridBounds],
            imagesFolder
          )
        case _ =>
          throw new DeserializationException("InputImage expected.")
      }
  }
}

case class JobRequest(id: String, target: String, inputImages: Seq[InputImage])

object JobRequest {
  implicit object JobRequestReader extends RootJsonReader[JobRequest] {
    def read(v: JsValue): JobRequest =
      v.asJsObject.getFields("jobId", "target", "input") match {
        case Seq(JsString(jobId), JsString(target), JsArray(inputImages)) =>
          JobRequest(
            jobId,
            target,
            inputImages
              .reverse
              .zipWithIndex
              .map { case (json, i) =>
                json.convertTo[InputImage].withPriority(i)
              }
          )
        case _ =>
          throw new DeserializationException("JobRequest expected.")
      }
  }
}
