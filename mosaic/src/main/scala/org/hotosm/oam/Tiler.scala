package org.hotosm.oam

import org.hotosm.oam.io._

import org.apache.spark._
import org.apache.spark.rdd._
import geotrellis.raster._
import geotrellis.spark._

import spire.syntax.cfor._

import ZoomUp.Implicits._

object Tiler {
  implicit class SaveWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def save(zoom: Int, createSink: () => Sink): RDD[(SpatialKey, OrderedImage)] =
      images.mapPartitions({ partition =>
        val sink = createSink()
        for(e @ (key, OrderedImage(tile, _)) <- partition) yield {
          sink(zoom, key, tile)
          e
        }
      }, preservesPartitioning = true)
  }

  implicit class ProcessBetweenWrapper(images: RDD[(SpatialKey, OrderedImage)]) {
    def processBetween(
      z1: Int,
      z2: Int,
      partitionerFactory: TilePartitionerFactory,
      createSink: () => Sink
    ): RDD[(SpatialKey, OrderedImage)] = {
      val merged =
        images
          .reduceByKey(OrderedImage.merge)
          .save(z1, createSink)

      val zoomed =
        merged.zoomUp(z1, partitionerFactory)

      if(z1 - 1 != z2) {
        zoomed.processBetween(z1 - 1, z2, partitionerFactory, createSink)
      } else {
        zoomed
      }
    }
  }

  def apply(inputImages: Seq[InputImageRDD])
           (createSink: () => Sink)
           (implicit sc: SparkContext): Unit = {
    val tileCounts =
      TileCounts(inputImages.map { ii => (ii.zoom, ii.gridBounds) })

    val partitionerFactory = TilePartitionerFactory(20, tileCounts)

    val zoomsToTiles: Map[Int, RDD[(SpatialKey, OrderedImage)]] =
      inputImages
        .map { case inputImage => (inputImage.zoom, inputImage.orderedImages) }
        .foldLeft(Map[Int, Seq[RDD[(SpatialKey, OrderedImage)]]]()) { (acc, tup) =>
          val (zoom, theseTiles) = tup
          val tiles =
            acc.get(zoom) match {
              case Some(t) => t ++ Seq(theseTiles)
              case None => Seq(theseTiles)
            }
          acc + ((zoom, tiles))
        }
        .map { case (zoom, rdds) => (zoom, sc.union(rdds)) }
        .toMap

    val sortedZooms =
      (zoomsToTiles.keys.toSeq :+ 1)
        .sortBy(-_)
        .toVector

    val zoomGroups =
      sortedZooms
        .sliding(2)

    val firstZoom = sortedZooms.head

    zoomGroups.foldLeft(None: Option[RDD[(SpatialKey, OrderedImage)]]) { (prev, zooms) =>
      val Seq(z1, z2) = zooms
      val tiles =
        prev match {
          case Some(t) => t.union(zoomsToTiles(z1))
          case None => zoomsToTiles(z1)
        }

      Some(
        tiles
          .partitionBy(partitionerFactory.unmergedAt(z1))
          .processBetween(z1, z2, partitionerFactory, createSink)
      )
    } match {
      case Some(rdd) => 
        rdd
          .save(1, createSink)
          .foreach { x => } // end with a dummy action to kick off the processing.
      case None =>
    }
  }
}
