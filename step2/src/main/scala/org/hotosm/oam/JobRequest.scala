package org.hotosm.oam

import geotrellis.vector._
import geotrellis.vector.io.json._
import geotrellis.raster._
import geotrellis.spark.tiling._
import spray.json._

class ZoomExtents(extents: Seq[(Int, Extent)]) {
  private val zoomPolygons =
    extents
      .groupBy(_._1)
      .map { case (zoom, elements) =>
        (zoom, MultiPolygon(elements.map(_._2.toPolygon)))
      }
      .toSeq

  def filter(filterExtent: Extent) =
    new ZoomExtents(
      extents.filter(_._2.intersects(filterExtent))
    )

  def mapZooms[T](f: (Int, MapKeyTransform, MultiPolygon) => T): Seq[T] =
    zoomPolygons.map { case (zoom, mp) =>
      f(zoom, TileMath.mapTransformFor(zoom), mp)
    }
}

case class InputImage(cellSize: CellSize, extent: Extent, imagesFolder: String, priority: Int = 0) {
  def maxZoom = TileMath.zoomFor(cellSize.width)
  def withPriority(p: Int) =
    InputImage(cellSize, extent, imagesFolder, p)
}

object InputImage {
  implicit object CellSizeReader extends JsonReader[CellSize] {
    def read(v: JsValue): CellSize =
      v.asJsObject.getFields("width", "height") match {
        case Seq(JsNumber(width), JsNumber(height)) =>
          CellSize(width.toDouble, height.toDouble)
        case _ =>
          throw new DeserializationException("CellSize expected.")
      }
  }

  implicit object InputImageReader extends RootJsonReader[InputImage] {
    def read(v: JsValue): InputImage =
      v.asJsObject.getFields("cellSize", "extent", "images") match {
        case Seq(cellSize, extent, JsString(imagesFolder)) =>
          InputImage(
            cellSize.convertTo[CellSize],
            extent.convertTo[Extent],
            imagesFolder
          )
        case _ =>
          throw new DeserializationException("InputImage expected.")
      }
  }
}

case class JobRequest(id: String, target: String, inputImages: Seq[InputImage], local: Boolean = false) {
  def zoomExtents: ZoomExtents =
    new ZoomExtents(
      inputImages.map { ii =>
        (ii.maxZoom, ii.extent)
      }
    )
}

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
              },
            !v.asJsObject.getFields("local").isEmpty
          )
        case _ =>
          throw new DeserializationException("JobRequest expected.")
      }
  }
}
