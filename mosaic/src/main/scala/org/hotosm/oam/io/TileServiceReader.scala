package org.hotosm.oam

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.io.Filesystem
import geotrellis.raster.io.geotiff._
import geotrellis.spark._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hadoop.formats._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
import org.apache.hadoop.fs.Path

import java.io.File
import scala.collection.JavaConversions._

trait TileServiceReader[T] {
  def read(zoom: Int): RDD[(SpatialKey, T)]
}

object TileServiceReader {
  val tiffExtensions = Array(".tif", ".TIF", ".tiff", ".TIFF")
  val TilePath = """.*/(\d+)/(\d+)/(\d+)\.\w+$""".r
}

trait ByteReader[T] extends Serializable {
  def read(bytes: Array[Byte]): T
}

class S3TileServiceReader[T: ByteReader](uri: String)(implicit sc: SparkContext) extends TileServiceReader[T] {
  import TileServiceReader._

  def read(zoom: Int): RDD[(SpatialKey, T)] = {
    val byteReader = implicitly[ByteReader[T]]

    val parsed = new java.net.URI(uri)
    val bucket = parsed.getHost
    val keys = {
      val path = parsed.getPath
      AWSClient.default.listKeys(bucket, path.substring(1, path.length))
        .map { key =>
          key match {
            case TilePath(z, x, y) if z.toInt == zoom => Some((SpatialKey(x.toInt, y.toInt), key))
            case _ => None
          }
        }
        .flatten
        .toSeq
    }

    val numPartitions = math.min(keys.size, math.max(keys.size / 10, 50)).toInt
    sc.parallelize(keys)
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitions({ partition =>
        val client = AWSClient.default

        partition.map { case (spatialKey, s3Key) =>

          (spatialKey, byteReader.read(client.readBytes(bucket, s3Key)))
        }
      }, preservesPartitioning = true)
  }
}

class FileTileServiceReader[T: ByteReader](uri: String)(implicit sc: SparkContext) extends TileServiceReader[T] {
  import TileServiceReader._

  def read(zoom: Int): RDD[(SpatialKey, T)] = {
    val paths = 
      FileUtils.listFiles(new File(uri), new SuffixFileFilter(tiffExtensions), TrueFileFilter.INSTANCE)
        .flatMap { file =>
          val path = file.getAbsolutePath
          path match {
            case TilePath(z, x, y) if z.toInt == zoom => Some((SpatialKey(x.toInt, y.toInt), path))
            case _ => None
          }
        }

    val numPartitions = math.min(paths.size, math.max(paths.size / 10, 50)).toInt
    val byteReader = implicitly[ByteReader[T]]
    sc.parallelize(paths.toSeq)
      .partitionBy(new HashPartitioner(numPartitions))
      .mapValues { path => byteReader.read(Filesystem.slurp(path)) }
  }
}
