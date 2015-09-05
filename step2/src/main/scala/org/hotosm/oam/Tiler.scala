package org.hotosm.oam

import org.apache.spark._
import org.apache.spark.rdd._

import geotrellis.raster._
import geotrellis.raster.mosaic._
import geotrellis.raster.io.geotiff._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._

import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.Files

import spray.json._
import spire.syntax.cfor._

object Tiler {
  def getSparkContext(): SparkContext = {
    val conf = 
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("HOT OSM Open Aerial Map Tiler")

    new SparkContext(conf)
  }

  implicit class CutTileWrapper(images: RDD[(Extent, MultiBandTile)]) {
    def cutTiles(zoomExtents: ZoomExtents): Seq[(Int, RDD[(SpatialKey, MultiBandTile)])] =
      zoomExtents.mapZooms { (z, mapTransform, multiPolygon) =>
        val tiles = 
          images
            .flatMap { case (extent, image) =>
              extent.safeIntersection(multiPolygon).asMultiPolygon.map { intersectionPolygons =>
                intersectionPolygons.polygons.toSeq.flatMap { polygon =>
                  mapTransform(polygon.envelope)
                    .coords
                    .map  { case (col, row) =>
                      val arr = Array.ofDim[ArrayTile](image.bandCount)
                      cfor(0)(_ < image.bandCount, _ + 1) { b =>
                        arr(b) = ArrayTile.empty(image.cellType, TileMath.TILE_DIM, TileMath.TILE_DIM)
                        arr(b).merge(mapTransform(col, row), extent, image.band(b))
                      }

                      (SpatialKey(col, row), ArrayMultiBandTile(arr): MultiBandTile)
                  }
                }
              }
            }
            .flatMap(identity)

        (z, tiles)
      }
  }

  implicit class SaveWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def save(zoom: Int, sink: (Int, SpatialKey, MultiBandTile) => Unit): RDD[(SpatialKey, OrderedImage)] =
      images.mapPartitions({ partition =>
        for(e @ (key, OrderedImage(tile, _)) <- partition) {
          sink(zoom, key, tile)
        }
        partition
      }, preservesPartitioning = true)
  }

  implicit class ZoomWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def zoomUp(thisZoom: Int): RDD[(SpatialKey, OrderedImage)] = {
      val partitioner = images.partitioner.get
      images
        .map { case (key, image) =>
          (SpatialKey(key.col / 2, key.row / 2), (key, image))
        }
        .groupByKey(partitioner)
        .mapPartitions({ partition =>
          val thisMapTransform = TileMath.mapTransformFor(thisZoom)
          val nextMapTransform = TileMath.mapTransformFor(thisZoom - 1)
          partition.map { case (nextKey, seqTiles) =>
            val nextExtent = nextMapTransform(nextKey)
            var image: MultiBandTile = 
              ArrayMultiBandTile(
                ByteArrayTile.empty(TileMath.TILE_DIM, TileMath.TILE_DIM),
                ByteArrayTile.empty(TileMath.TILE_DIM, TileMath.TILE_DIM),
                ByteArrayTile.empty(TileMath.TILE_DIM, TileMath.TILE_DIM)
              )
            var maxOrder = 0
            for((thisKey, thisTile) <- seqTiles) {
              val thisExtent = thisMapTransform(thisKey)
              if(thisTile.order > maxOrder) maxOrder = thisTile.order
              image = image.merge(nextExtent, thisExtent, thisTile)
            }
            (nextKey, OrderedImage(image, maxOrder))
          }
        }, preservesPartitioning = true)
    }
  }

  implicit class ProcessBetweenWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def processBetween(z1: Int, z2: Int, sink: (Int, SpatialKey, MultiBandTile) => Unit): RDD[(SpatialKey, OrderedImage)] = {
      val merged = 
        images
          .reduceByKey(OrderedImage.merge)
          .save(z1, sink)

      if(z1 - 1 != z2) {
        merged
          .zoomUp(z1)
          .processBetween(z1 - 1, z2, sink)
      } else { 
        merged 
      }
    }
  }

  def apply(inputImages: Seq[(Int, Extent, RDD[(Extent, MultiBandTile)])], zoomExtents: ZoomExtents)
           (sink: (Int, SpatialKey, MultiBandTile) => Unit)
           (implicit sc: SparkContext): Unit = {
    val zoomsToTiles: Map[Int, RDD[(SpatialKey, OrderedImage)]] =
      inputImages
        .flatMap { case (priority, overallExtent, images) =>
          images
            .cutTiles(zoomExtents.filter(overallExtent))
            .map { case (zoom, tiles) =>
              (zoom,
                tiles
                  .partitionBy(new HashPartitioner(20))
                  .mapValues(OrderedImage(_, priority))
              )
            }
        }
        .foldLeft(Map[Int, RDD[(SpatialKey, OrderedImage)]]()) { (acc, tup) =>
          val (zoom, theseTiles) = tup
          val tiles =
            acc.get(zoom) match {
              case Some(t) => t.union(theseTiles)
              case None => theseTiles
            }
          acc + ((zoom, tiles))
        }

    val zoomGroups =
      (zoomsToTiles.keys.toSeq :+ 0)
        .sortBy(-_)
        .sliding(2)

    zoomGroups.foldLeft(None: Option[RDD[(SpatialKey, OrderedImage)]]) { (prev, zooms) =>
      val Seq(z1, z2) = zooms
      val tiles =
        prev match {
          case Some(t) => t.union(zoomsToTiles(z1))
          case None => zoomsToTiles(z1)
        }

      Some(tiles.processBetween(z1, z2, sink))
    } match {
      case Some(rdd) => 
        // Action to kick off process
        rdd.count
      case None =>
    }
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
      val inputImages: Seq[(Int, Extent, RDD[(Extent, MultiBandTile)])] =
        jobRequest.inputImages
          .map { ii =>
           (ii.priority, ii.extent, ImageRDD(ii.imagesFolder))
          }

      val sink = 
        if(jobRequest.local)
          new LocalSink(jobRequest.target)
        else
          new S3Sink(client, jobRequest.target)

      apply(inputImages, jobRequest.zoomExtents)(sink)

    } finally {
      sc.stop
    }
  }
}
