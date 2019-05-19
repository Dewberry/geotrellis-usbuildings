package usbuildings

import java.net.URL

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.raster.summary.polygonal.{DoubleHistogramSummary, MaxSummary}
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import geotrellis.raster._
import geotrellis.spark.SpatialKey
import geotrellis.spark.io._
import geotrellis.spark.tiling.ZoomedLayoutScheme
import geotrellis.vectortile.VectorTile
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.CoGroupedRDD
import org.locationtech.jts.geom.TopologyException

import scala.util.{Try, Success, Failure}

class BuildingSummaryApp(
  val buildingsUri: Seq[String],
  val layers: Map[String, Layer],
  val zoom: Int,
  val sampleFraction: Option[Double] = None
)(@transient implicit val sc: SparkContext) extends LazyLogging with Serializable {
  import Implicits._

  /** Explode each layer so we can parallelize reading over layers */
  val allBuildings: RDD[Building] = sampleFraction match {
    case Some(sf) =>
      sc.parallelize(buildingsUri, buildingsUri.length)
      .flatMap { url =>
        Building.readFromGeoJson(new URL(url))
      }.sample(withReplacement = false, sf)

    case None =>
      sc.parallelize(buildingsUri, buildingsUri.length)
        .flatMap { url =>
          Building.readFromGeoJson(new URL(url))
        }.repartition(buildingsUri.length * 32)

  }

  val partitioner = new HashPartitioner(partitions=allBuildings.getNumPartitions * 4) //ravi commenting off partitions to let spark determine optimal

  val summaries: Map[String, RDD[(Id, StreamingHistogram)]] = {
    val summaryFn: (Try[Raster[MultibandTile]], Polygon) => Option[StreamingHistogram] =
      { (rasterTry, geom) =>
        rasterTry.map { raster =>
          // mapping over Try will capture possible Geometry intersection errors and fold them into Failure
          // -- instead of throwing and interrupting the full job
          // Note: we're only using band 0 of the layer here
          val ret = raster.tile.band(0).polygonalSummary(raster.extent, geom, CustomDoubleHistogramSummary)
          ret.minMaxValues.map( _ => ret) // if we only got NODATA, drop the result early
        } match {
          case Success(ret) =>
            ret
          case Failure(e: TopologyException) =>
            logger.error(s"${e.getMessage} : ${WKT.write(geom)}")
            None
          case Failure(e: ValueNotFoundError) =>
            None // tile wasn't there, sad but sometimes expected
          case Failure(e: Throwable) =>
            logger.error(e.getMessage)
            None
        }
      }

    layers.map { case (name, layer) =>
      val features = allBuildings.map { b => Feature(b.footprint, b.id) }
      (name, FeatureSummaryRDD.summaryByLayer(features, LatLng, layer, summaryFn, partitioner))
    }
  }


  // -- Join Summary statistics to building records by Id
  /* Note:
  Usage of CoGroupRDD here is to allow variable number of summaries to be joined to the result.
  This produces unsightly and dangerous .asInstanceOf calls.
  However there is performance adventages to join in single step vs chaining RDD joins.
  */
  val buildingsById = allBuildings.map({ building => (building.id, building) })
  val (names: List[String], rdds: List[RDD[(Id, StreamingHistogram)]]) = summaries.toList.unzip
  val cogrouped: CoGroupedRDD[Id] = new CoGroupedRDD(buildingsById :: rdds.toList, partitioner)

  val buildingsWithHistograms: RDD[Building] =
    cogrouped.map { case (fid, row) =>
      val rowArr: Array[Iterable[Any]] = row.toArray
      val building = rowArr.head.asInstanceOf[Iterable[Building]].head
      val summaryHistogram = rowArr.tail.asInstanceOf[Array[Iterable[StreamingHistogram]]]
      val perLayerSummaries = names.zip(summaryHistogram).toMap.mapValues(_.headOption)
      building.copy(histograms = perLayerSummaries)
    }

  // -- Reproject Building geometries to WebMercator and save them as Vector tiles
  val layoutScheme = ZoomedLayoutScheme(WebMercator)
  val layout = layoutScheme.levelForZoom(zoom).layout

  val buildingsPerWmTile: RDD[(SpatialKey, Iterable[Building])] =
  buildingsWithHistograms.flatMap { building =>
      val wmFootprint = building.footprint.reproject(LatLng, WebMercator)
      val wmBuilding = building.copy(footprint = wmFootprint)
      val layoutKeys: Set[SpatialKey] = layout.mapTransform.keysForGeometry(wmFootprint)
      layoutKeys.toIterator.map( key => (key, wmBuilding))
    }.groupByKey(partitioner)

  def tiles: RDD[(SpatialKey, VectorTile)] =
    buildingsPerWmTile.map { case (key, buildings) =>
      val extent = layout.mapTransform.keyToExtent(key)
      (key, Util.makeVectorTile(extent, buildings))
    }
}
