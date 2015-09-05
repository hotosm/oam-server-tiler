package org.hotosm.oam

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
import org.apache.hadoop.fs.Path

object ImageRDD {
  private val tiffExtensions = Seq(".tif", ".TIF", ".tiff", ".TIFF")

  class ImageInputFormat extends S3InputFormat[Extent, MultiBandTile] {
    def createRecordReader(split: InputSplit, context: TaskAttemptContext) =
      new S3RecordReader[Extent, MultiBandTile] {
        def read(bytes: Array[Byte]) = {
          val geoTiff = MultiBandGeoTiff(bytes)
          (geoTiff.extent, geoTiff.tile)
        }
      }
  }

  def apply(path: String)(implicit sc: SparkContext): RDD[(Extent, MultiBandTile)] = {
    val searchPath = path match {
      case p if tiffExtensions.exists(p.endsWith) => new Path(path)
      case p =>
        val extensions = tiffExtensions.mkString("{", ",", "}")
        new Path(s"$p/*$extensions")
    }

    val updatedConf =
      sc.hadoopConfiguration.withInputDirectory(searchPath)

    sc.newAPIHadoopRDD(
      updatedConf,
      classOf[MultiBandGeoTiffInputFormat],
      classOf[ProjectedExtent],
      classOf[MultiBandTile]
    ).map { case (key, value) => (key.extent, value) }
  }

  def apply(bucket: String, key: String, splitSize: Option[Int] = None)(implicit sc: SparkContext): RDD[(Extent, MultiBandTile)] = {
    val job = Job.getInstance(sc.hadoopConfiguration, "S3 GeoTiff ETL")
    S3InputFormat.setBucket(job, bucket)
    S3InputFormat.setPrefix(job, key)
    if (splitSize.isDefined)
      S3InputFormat.setMaxKeys(job, splitSize.get)
    val conf = job.getConfiguration
    sc.newAPIHadoopRDD(conf, classOf[ImageInputFormat], classOf[Extent], classOf[MultiBandTile])
  }
}
