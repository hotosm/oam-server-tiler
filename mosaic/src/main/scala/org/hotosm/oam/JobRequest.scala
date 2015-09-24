package org.hotosm.oam

import org.hotosm.oam.io._

import org.apache.spark._
import geotrellis.raster._
import spray.json._

case class JobRequest(id: String, target: String, tileSize: Int, inputImageDefinitions: Seq[InputImageDefinition]) {
  def inputImages(implicit sc: SparkContext): Seq[InputImageRDD] = {
    inputImageDefinitions
      .map { case InputImageDefinition(_, zoom, gridBounds, imagesFolder, priority) =>
        val reader =
          new java.net.URI(imagesFolder).getScheme match {
            case "s3" => new S3TileServiceReader[MultiBandTile](imagesFolder)
            case null => new FileTileServiceReader[MultiBandTile](imagesFolder)
          }

        val rdd = reader.read(zoom)

        if(tileSize == TileMath.TILE_DIM) {
          InputImageRDD(priority, zoom, gridBounds, rdd)
        } else {
          if((tileSize / 256.0) != math.floor(tileSize / 256.0)) { sys.error("tileSize is required to be a multiple of 256") }
          val tilesAcross = tileSize / 256
          val zoomDiff = {
            val d = math.log(tilesAcross) / math.log(2)
            if(d != math.floor(d)) { sys.error("tileSize is required to a be a power of 2 * 256") }
            d.toInt
          }
          val adjustedZoom = zoom + zoomDiff
          val adjustedGridBounds =
            GridBounds(gridBounds.colMin / tilesAcross, gridBounds.rowMin / tilesAcross, gridBounds.colMax / tilesAcross, gridBounds.rowMax / tilesAcross)

          val images =
            rdd
              .flatMap { case (key, image) =>
                InputImageRDD.split(key, image, tilesAcross)
              }
          InputImageRDD(priority, adjustedZoom, adjustedGridBounds, images)
        }
      }
  }
}

object JobRequest {
  implicit object JobRequestReader extends RootJsonReader[JobRequest] {
    def read(v: JsValue): JobRequest =
      v.asJsObject.getFields("jobId", "target", "tileSize", "input") match {
        case Seq(JsString(jobId), JsString(target), JsNumber(tileSize), JsArray(inputImages)) =>
          JobRequest(
            jobId,
            target,
            tileSize.toInt,
            inputImages
              .reverse
              .zipWithIndex
              .map { case (json, i) =>
                json.convertTo[InputImageDefinition].withPriority(i)
              }
          )
        case _ =>
          throw new DeserializationException("JobRequest expected.")
      }
  }
}
