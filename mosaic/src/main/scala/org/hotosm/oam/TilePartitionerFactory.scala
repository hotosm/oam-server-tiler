package org.hotosm.oam

import org.apache.spark._

trait TilePartitionerFactory {
  def mergedAt(zoom: Int): Partitioner
  def unmergedAt(zoom: Int): Partitioner
}

object TilePartitionerFactory {
  def apply(tilesPerPartition: Int, tileCounts: TileCounts)(implicit sc: SparkContext): TilePartitionerFactory = {
    val defaultParallelism = sc.defaultParallelism
    new TilePartitionerFactory {
      def mergedAt(zoom: Int) = new HashPartitioner(
        math.max(tileCounts.mergedAt(zoom) / tilesPerPartition, defaultParallelism).toInt
      )
      def unmergedAt(zoom: Int) = new HashPartitioner(
        math.max(tileCounts.unmergedAt(zoom) / tilesPerPartition, defaultParallelism).toInt
      )
    }
  }
}
