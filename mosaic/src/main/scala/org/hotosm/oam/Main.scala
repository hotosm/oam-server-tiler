package org.hotosm.oam

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
    assert(args.length == 1, "Arguement error: must only be one argument, that is the URI to the request JSON")

    val client = AWSClient.default

    val jobRequest = {
      val uri = args(0)
      new java.net.URI(uri).getScheme match {
        case null =>
          scala.io.Source.fromFile(uri).getLines.mkString.parseJson.convertTo[JobRequest]
        case _ =>
          client.readTextFile(uri).parseJson.convertTo[JobRequest]
      }
    }

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

    } finally {
      sc.stop
    }
  }
}
