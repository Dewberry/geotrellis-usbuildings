package usbuildings

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.{MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.spark.SpatialKey
import geotrellis.vector._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import cats.implicits._
import geotrellis.raster.histogram._
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.spark.{LayerId, TileLayerMetadata}
import geotrellis.spark.io._
import geotrellis.proj4._
import scala.util._
import cats._
import cats.implicits._
import scala.reflect.ClassTag
import org.apache.spark.rdd.CoGroupedRDD

/** Reference to a GeoTrellis layer
 * @param catalog URI for GeoTrellis catalog (Ex: s3://bucket/catalog)
 * @param layerId Layer identifier in above catalog to use for raster data
 */
case class Layer(catalog: String, layerId: LayerId)

object FeatureSummaryRDD extends LazyLogging {

  /** Intersect RDD of features with stored GeoTrellis layer and capture summary statistics.
   *
   * Feature geometries will be reprojected to layer projection before invoking summary function.
   * Generating this summary per layer allows combining results from layers that do not share tile layout or projection.
   *
   * `summaryFn` is given a Try of raster so it has option to capture some failures in feature attributes.
   * If `summaryFn` returns `None` the corresponding feature, attribute will be dropped from result RDD.
   * Features that do not intersect layer tiles will be dropped from result RDD.
   *
   * @tparam I Type of the feature identiy, used to group results
   * @tparam R Type of per-feature summary result
   * @param features RDD of features to be intersected with GeoTrellis layer
   * @param featureCrs CRS of feature geometries, may be used to reproject features to layer projection
   * @param layer Reference to GeoTrellis layer
   * @param summaryFn Function to compute summary from tile, feature tuple that intersect
   * @param partitioner Partitioner on I for the result of the summaries
   */
  def summaryByLayer[I: ClassTag, R: Semigroup: ClassTag](
    features: RDD[Feature[Polygon, I]],
    featureCrs: CRS,
    layer: Layer,
    summaryFn: (Try[Raster[Tile]], Polygon) => Option[R],
    partitioner: Partitioner
  ): RDD[(I, R)] = {
    // Get layer matadata so we can know how it is gridded
    val store = AttributeStore(layer.catalog)
    val tlm = store.readMetadata[TileLayerMetadata[SpatialKey]](layer.layerId)

    val projectedFeatures =
      if (featureCrs != tlm.crs)
        features.map(_.mapGeom(_.reproject(featureCrs, tlm.crs)))
      else
        features

    val keyedFeatures: RDD[(SpatialKey, Feature[Polygon, I])] =
      projectedFeatures.flatMap { feature =>
        if (! tlm.extent.intersects(feature.geom)) {
          // filter out geometries that don't intersect layer data extent
          Seq.empty
        } else {
          // intersect geometries with layer grid and make a recrod for each intersection
          val featureKeys = tlm.layout.mapTransform.keysForGeometry(feature.geom)
          featureKeys.map { key =>
            (key, feature)
          }
        }
      }

    // group all polygons by key so we can read a tile only once
    val grouped: RDD[(SpatialKey, Iterable[Feature[Polygon, I]])] =
      keyedFeatures.groupByKey(numPartitions = keyedFeatures.getNumPartitions * 8)
    // This groupByKey may need higher partitioning number to be peformant.
    // Default will be the same number of partitions as input RDD, 1 partitioner per state

    val summariesByKey: RDD[(I, R)] =
      grouped.mapPartitions ({ it =>
        val reader: Reader[SpatialKey, Tile] = ValueReader(layer.catalog).reader[SpatialKey, Tile](layer.layerId)

        it.flatMap { case (key, features) =>
          // read tile once and use it for all tiles, will throw if tile is missing
          val raster: Try[Raster[Tile]] =
            Try(reader(key)).map { tile: Tile =>
              val extent = key.extent(tlm.layout)
              Raster(tile, extent)
            }

          features.flatMap { case Feature(geom, fid) =>
            summaryFn(raster, geom).map({ result => (fid, result) })
          }
        }
     })

    // reduce across multiple layout keys that intersected each feature
    summariesByKey.reduceByKey(partitioner, Semigroup[R].combine(_, _))
      .setName(s"Summary ${layer.layerId}")
  }
}