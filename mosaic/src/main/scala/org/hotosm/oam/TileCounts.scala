package org.hotosm.oam

import org.apache.spark._
import geotrellis.raster._

import scala.collection.mutable

trait TileCounts {
  def unmergedAt(zoom: Int): Int
  def mergedAt(zoom: Int): Int
}

object TileCounts {
  def apply(zoomsToGridBounds: Seq[(Int, GridBounds)]): TileCounts = {
    val imageCount = zoomsToGridBounds.size
    val zoomMap =
      zoomsToGridBounds
        .foldLeft(Map[Int, Seq[GridBounds]]()) { (acc, tup) =>
          val (zoom, gridBounds) = tup
          val bounds =
            acc.get(zoom) match {
              case Some(seq) => seq :+ gridBounds
              case None => Seq(gridBounds)
            }
          acc + ((zoom, bounds))
        }

    val maxZoom = zoomMap.keys.max

    val (_, mergedCounts, unmergedCounts) =
      (maxZoom until 1 by -1)
        .foldLeft((Seq[GridBounds](), Map[Int, Int](), Map[Int, Int]())) { (acc, zoom) =>
          val (lastBounds, mergedCounts, unmergedCounts) = acc
          val zoomedLastBounds =
            lastBounds.map { gb =>
              GridBounds(gb.colMin / 2, gb.rowMin / 2, gb.colMax / 2, gb.rowMax / 2)
            }

          val bounds =
            zoomMap.get(zoom) match {
              case Some(boundsSeq) => zoomedLastBounds ++ boundsSeq
              case None => zoomedLastBounds
            }

          val distinctBounds =
            try {
              GridBounds.distinct(bounds)
            } catch {
              case e: Exception =>
                println(s"BOUNDS: $bounds")
                throw e
            }

          (
            distinctBounds,
            mergedCounts + ((zoom, distinctBounds.map(_.size).sum * imageCount)),
            unmergedCounts + ((zoom, bounds.map(_.size).sum * imageCount))
          )
        }

    new TileCounts {
      def unmergedAt(zoom: Int): Int =
        unmergedCounts.getOrElse(zoom, 0)

      def mergedAt(zoom: Int): Int =
        mergedCounts.getOrElse(zoom, 0)
    }
  }
}
