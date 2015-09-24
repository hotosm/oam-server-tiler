package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.io.json._
import spray.json._

case class InputImageDefinition(sourceUri: String, zoom: Int, gridBounds: GridBounds, imagesFolder: String, priority: Int = 0) {
  def withPriority(p: Int) =
    InputImageDefinition(sourceUri, zoom, gridBounds, imagesFolder, p)
}

object InputImageDefinition {
  implicit object InputImageDefinitionReader extends RootJsonReader[InputImageDefinition] {
    def read(v: JsValue): InputImageDefinition =
      v.asJsObject.getFields("sourceUri", "zoom", "gridBounds", "tiles") match {
        case Seq(JsString(sourceUri), JsNumber(zoom), gridBounds, JsString(imagesFolder)) =>
          InputImageDefinition(
            sourceUri,
            zoom.toInt,
            gridBounds.convertTo[GridBounds],
            imagesFolder
          )
        case _ =>
          throw new DeserializationException("InputImageDefinition expected.")
      }
  }
}
