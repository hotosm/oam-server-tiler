package org.hotosm.oam

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.ingest._
import geotrellis.spark.utils._

import org.apache.spark._
import spire.syntax.cfor._

import org.scalatest._
import geotrellis.testkit._

// object TestSparkContext {
//   lazy val _sc = {
//     System.setProperty("spark.driver.port", "0")
//     System.setProperty("spark.hostPort", "0")

//     val sparkContext = SparkUtils.createLocalSparkContext("local[*]", "Test Context", new SparkConf())

//     System.clearProperty("spark.driver.port")
//     System.clearProperty("spark.hostPort")

//     sparkContext
//   }
// }

// trait TestSparkContext {
//   implicit def sc: SparkContext = TestSparkContext._sc
// }

// object TilerSpec {
//   val TILE_DIM = 256
//   val layoutScheme = ZoomedLayoutScheme(TILE_DIM)

//   def getGreenValue(index: Int): Byte =
//     (index * 10 + 1).toByte

//   def buildImage(index: Int): MultiBandTile = {
//     val greenValue= getGreenValue(index)
//     ArrayMultiBandTile(
//       ArrayTile(Array.ofDim[Byte](TILE_DIM * TILE_DIM).fill((greenValue - 1).toByte), TILE_DIM, TILE_DIM),
//       ArrayTile(Array.ofDim[Byte](TILE_DIM * TILE_DIM).fill(greenValue), TILE_DIM, TILE_DIM),
//       ArrayTile(Array.ofDim[Byte](TILE_DIM * TILE_DIM).fill((greenValue + 1).toByte), TILE_DIM, TILE_DIM)
//     )
//   }

//   def getMapTransform(zoom: Int): MapKeyTransform = {
//     val LayoutLevel(_, tileLayout) = layoutScheme.levelFor(zoom)
//     MapKeyTransform(WebMercator.worldExtent, tileLayout.layoutCols, tileLayout.layoutRows)
//   }


//   val mapTransform12 = getMapTransform(12)
//   val mapTransform10 = getMapTransform(10)

//   val baseExtent = mapTransform10(205, 306)
//   val image1Extent = {
//     val (dx, dy) = (baseExtent.width / 2, baseExtent.height / 2)
//     Extent(
//       baseExtent.xmin - dx,
//       baseExtent.ymin - dy,
//       baseExtent.xmax + dx,
//       baseExtent.ymax + dy
//     )
//   }

//   val image2Extent = {
//     val (dx, dy) = (baseExtent.width / 6, baseExtent.height / 6)
//     Extent(
//       baseExtent.xmin + dx,
//       baseExtent.ymax - (3 * dy),
//       baseExtent.xmin + (3 * dx),
//       baseExtent.ymax - dy
//     )
//   }

//   val image3Extent = {
//     val (dx, dy) = (baseExtent.width / 6, baseExtent.height / 6)
//     Extent(
//       baseExtent.xmin + (2 * dx),
//       baseExtent.ymax - (4 * dy),
//       baseExtent.xmin + (4 * dx),
//       baseExtent.ymax - (2 * dy)
//     )
//   }

//   val image4Extent = {
//     val (dx, dy) = (baseExtent.width / 6, baseExtent.height / 6)
//     Extent(
//       baseExtent.xmin + (3 * dx),
//       baseExtent.ymax - (5 * dy),
//       baseExtent.xmin + (5 * dx),
//       baseExtent.ymax - (3 * dy)
//     )
//   }

//   def input(implicit sc: SparkContext) =
//     Seq(
//       (0, image1Extent, sc.parallelize(Seq((image1Extent, buildImage(1))))),
//       (1, image2Extent.combine(image3Extent).combine(image4Extent),
//         sc.parallelize(
//           Seq(
//             (image2Extent, buildImage(2)),
//             (image3Extent, buildImage(3)),
//             (image4Extent, buildImage(4))
//           )
//         )
//       )
//     )

//   def expectedGreen(zoom: Int, key: SpatialKey): Tile = {
//     val result = ByteArrayTile.empty(TILE_DIM, TILE_DIM)
//     val mapTransform = getMapTransform(zoom)
//     val resultExtent = mapTransform(key)
//     val re = RasterExtent(resultExtent, TILE_DIM, TILE_DIM)
//     for((extent, i) <- Seq(image1Extent, image2Extent, image3Extent, image4Extent).zipWithIndex) {
//       if(extent.intersects(resultExtent)) {
//         val greenValue = getGreenValue(i + 1)
//         Rasterizer.foreachCellByPolygon(extent, re) { (col, row) =>
//           result.set(col, row, greenValue)
//         }
//       }
//     }

//     result
//   }
// }

// class TilerSpec extends FunSuite with Matchers with RasterMatchers with TestSparkContext {
//   import TilerSpec._

//   test("The tiling process") {
//     val acc = sc.accumulator(0)
//     Tiler.apply(input) { (zoom, key, tile) =>
//       acc += 1
//       val expected = expectedGreen(zoom, key)
//       val actual = tile.band(1)

//       if(expected.cols != actual.cols) { sys.error("Cols are not equal") }
//       if(expected.rows != actual.rows) { sys.error("Rows are not equal") }
//       cfor(0)(_ < actual.rows, _ + 1) { row =>
//         cfor(0)(_ < actual.cols, _ + 1) { col =>
//           val v1 = actual.get(col, row)
//           val v2 = expected.get(col, row)
//           if(v1 != v2) { sys.error("Failed on $col, $row: $v1 != $v2") }
//         }
//       }
//     }

//     println(s"TILER TEST: ${acc.value} tiles sunk")
//   }
// }
