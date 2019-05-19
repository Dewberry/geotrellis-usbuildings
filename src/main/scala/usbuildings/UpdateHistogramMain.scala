package usbuildings

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.raster.histogram._
import geotrellis.vector._
import geotrellis.raster.resample.Bilinear
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.gdal._
import geotrellis.contrib.vlm.spark._
import geotrellis.spark.io.index._
import geotrellis.spark.tiling._
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import spray.json.DefaultJsonProtocol._
import scala.util.{Properties, Try, Success, Failure}
import cats.data.Op


/** Updated or replace histogram based on ingest GeoTrellis layer
 * Histogram will be stored at zoom 0 of the layer
 */
object UpdateHistogramMain extends CommandApp(
  name = "update-histogram",
  header = "",
  main = {
    val catalogOpt = Opts.option[URI]("catalog", help = "URI for GeoTrellis catalog")
      .withDefault(new URI("s3://dewberry-demo/testingests"))

    val layerNameOpt = Opts.option[String]("layer", help = "Layer name in above catalog")

    val zoomOpt = Opts.option[Int]("zoom", help = "Zoom level to sample")

    val replaceOpt = Opts.flag("replace", help = "Force replacement, otherwise merge with existing").orFalse

    val partitionsOpt = Opts.option[Int]("partitions", help = "Number of partitions, default is to estimate").orNone

    (catalogOpt, layerNameOpt, zoomOpt, replaceOpt, partitionsOpt).mapN {
      (catalogUri, layerName, zoom, replace, numPartitions) =>

      val layerId = LayerId(layerName, zoom)
      println(s"Catalog: $catalogUri")
      println(s"Layer: $LayerId")

      //to solve timeout problem
      System.setProperty("sun.net.client.defaultReadTimeout", "60000")

      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Update Histogram").
        set("spark.serializer", classOf[KryoSerializer].getName).
        set("spark.kryo.registrator", classOf[KryoRegistrator].getName).
        set("spark.executionEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate
      implicit val sc: SparkContext = ss.sparkContext

      val attributeStore = AttributeStore(catalogUri)

      val reader = LayerReader(attributeStore, catalogUri)
      val jobNumPartitions = numPartitions.getOrElse(sc.defaultParallelism)

      val layer = reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId, jobNumPartitions)
      val updateHistograms: Array[Histogram[Double]] = layer.histogram
      val topLayerId = LayerId(layerName, 0)

      if (replace) {
        attributeStore.write(topLayerId, "histogram", updateHistograms)
      } else {
        Try(attributeStore.read[Array[Histogram[Double]]](topLayerId, "histogram")) match {
          case Success(savedHistograms) =>
            // zip and consider edge case that band count may differ
            val zipped = savedHistograms.map(Option.apply).zipAll(updateHistograms.map(Option.apply), None, None)
            val mergedHistograms = zipped.map {
              case (Some(sh), Some(uh)) => StreamingHistogram(sh.bucketCount).merge(sh).merge(uh): Histogram[Double]
              case (Some(sh), None) => sh
              case (None, Some(uh)) => uh
              case (None, None) => sys.error("No saved or generated histogram")
            }
            val oldMinMax = savedHistograms.map(_.minMaxValues()).toList
            val updMinMax = updateHistograms.map(_.minMaxValues()).toList
            val mrgMinMax = mergedHistograms.map(_.minMaxValues()).toList
            println(s"Updating histogram for $topLayerId, old: $oldMinMax upd: $updMinMax mrg: $mrgMinMax")
            attributeStore.write(topLayerId, "histogram", mergedHistograms)

          case Failure(e: AttributeNotFoundError) =>
            println(s"Saving histogram for $topLayerId, new: ${updateHistograms.map(_.minMaxValues()).toList}")
            attributeStore.write(topLayerId, "histogram", updateHistograms)

          case Failure(e) =>
            sys.error(s"Failed to read saved histogram: $e")
            // Couldn't read for replace, but still lets try to update
            attributeStore.write(topLayerId, "histogram", updateHistograms)
        }
      }
    }
  }
)
