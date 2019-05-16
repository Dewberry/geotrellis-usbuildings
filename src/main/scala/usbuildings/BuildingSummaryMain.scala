package usbuildings

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.proj4.WebMercator
import geotrellis.raster.MultibandTile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.io.s3.{S3AttributeStore, S3GeoTiffRDD, S3LayerManager, S3LayerWriter}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark._
//import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.spark.io._
import geotrellis.spark.{Metadata, _}
import spray.json.DefaultJsonProtocol._

import scala.util.Properties
import cats.data.Validated
import com.typesafe.scalalogging.LazyLogging

// This is an example showing the checker boxes problem with 10m NEDs.
object BuildingSummaryMain extends CommandApp(
  //Name for runtime
  name = "building-summary",
  header = "",
  main = {
    val buildingsOpt = Opts.options[String](
      long = "buildings",
      help = "URI of the building shapes layers, if not given, read all buildings from Bing repo"
    ).orNone

    val layersOpt = Opts.options[String](
      long = "layers",
      help = "GeoTrellis layers for summary: {attribute-name},{catalog-url},{layer-name},{zoom}"
    ).mapValidated { stringNel =>
      val Rx = """(\w+),(.+),(\w+),(\d+)""".r
      stringNel.map { string =>
        string match {
          case Rx(attribute, catalogUri, layerName, zoom) =>
            Validated.valid((attribute, Layer(catalogUri, LayerId(layerName, zoom.toInt))))
          case _ =>
            Validated.invalidNel(s"Invalid layer: $string")
        }
      }.sequence
    }

    val outputOpt = Opts.option[String]("output",
      help = "S3 URI prefix of output tiles"
    ).withDefault("s3://geotrellis-test/usbuildings/default")

    (buildingsOpt, outputOpt, layersOpt).mapN { (buildingsUri, outputUriString, layers) =>
      val outputUri = new URI(outputUriString)
      if (outputUri.getScheme != "s3") {
        throw new java.lang.IllegalArgumentException("--output must be an S3 URI")
      }
      val bucket = outputUri.getHost
      val path = outputUri.getPath.stripPrefix("/")
      val layersMap = layers.toList.toMap
      layersMap.foreach { case (attribute, layer) =>
        println(s"Attributes: $attribute -> $layer ")
      }

      val conf = new SparkConf().
        setIfMissing("spark.master", "local[*]").
        setAppName("Building Footprint Elevation").
        set("spark.serializer", classOf[KryoSerializer].getName).
        set("spark.kryo.registrator", classOf[KryoRegistrator].getName).
        set("spark.executionEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))

      implicit val ss: SparkSession = SparkSession.builder
        .config(conf)
        .enableHiveSupport
        .getOrCreate
      implicit val sc: SparkContext = ss.sparkContext

      val uris: List[String] = buildingsUri.map(_.toList).getOrElse(Building.geoJsonURLs)
      val buildings = new BuildingSummaryApp(uris, layersMap)

      GenerateVT.save(buildings.tiles, zoom= 15, bucket, path)
    }
  }
) with LazyLogging
