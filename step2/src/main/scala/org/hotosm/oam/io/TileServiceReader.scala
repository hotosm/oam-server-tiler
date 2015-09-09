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

trait ByteReader[T] extends Serializable {
  def read(bytes: Array[Byte]): T
}

object TileServiceReader {
  def apply[T: ByteReader](uri: String)(implicit sc: SparkContext): TileServiceReader[T] = {
    new java.net.URI(uri).getScheme match {
      case "http" => ???//HttpTileServiceReader[T](uri)
      case "s3" => ???//S3TileServiceReader[T](uri)
      case "hdfs" => ???//HadoopTileServiceReader[T](uri)
      case null => new FileTileServiceReader[T](uri)
    }
  }
}

class FileTileServiceReader[T: ByteReader](uri: String)(implicit sc: SparkContext) extends TileServiceReader[T] {
  private val tiffExtensions = Array(".tif", ".TIF", ".tiff", ".TIFF")
  private val TilePath = """.*/(\d+)/(\d+)/(\d+)\.\w+$""".r

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
