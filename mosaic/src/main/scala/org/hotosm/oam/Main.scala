package org.hotosm.oam

import org.hotosm.oam.io._

import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.raster._
import geotrellis.spark._

import spray.json._

object Main {
  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setIfMissing("spark.master", "local[8]")
        .setIfMissing("spark.app.name", "HOT OSM Open Aerial Map Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    new SparkContext(conf)
  }

  def main(args: Array[String]): Unit = {
    val publishNotifications =
      args.length != 2

    if(args.length < 1) {
      sys.error("Argument error: must give the URI to the step1 result JSON.")
    }

    val jobRequest = {
      val uri = args(0)
      new java.net.URI(uri).getScheme match {
        case null =>
          scala.io.Source.fromFile(uri).getLines.mkString.parseJson.convertTo[JobRequest]
        case _ =>
          S3Client.default.readTextFile(uri).parseJson.convertTo[JobRequest]
      }
    }

    if(publishNotifications) { Status.notifyStart(jobRequest.id) }

    implicit val sc = getSparkContext()

    try {
      val inputImages: Seq[InputImageRDD] =
        jobRequest.inputImages

      val createSink: () => Sink = {
        val parsedTarget = new java.net.URI(jobRequest.target)
        parsedTarget.getScheme match {
          case null => 
            { () => new LocalSink(jobRequest.target) }
          case "s3" =>
            val bucket = parsedTarget.getHost
            val path = parsedTarget.getPath
            val key = path.substring(1, path.length)

            { () => new S3Sink(bucket, key) }
          case x =>
            throw new NotImplementedError(s"No sink implemented for scheme $x")
        }
      }

      Tiler(inputImages)(createSink)
    } catch {
      case e: Exception =>
        if(publishNotifications) { Status.notifyFailure(jobRequest.id, e) }
        throw e
    } finally {
      sc.stop
    }

    if(publishNotifications) { Status.notifySuccess(jobRequest.id, jobRequest.target, jobRequest.inputImageDefinitions.map(_.sourceUri)) }
  }
}
