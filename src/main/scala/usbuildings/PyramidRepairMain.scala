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


/** Job to regenerate a Pyramid or sub-extent of pyramid from given zoom level up */
object PyramidRepairMain extends CommandApp(
  name = "pyramid-repair",
  header = "",
  main = {
    val catalogOpt = Opts.option[URI]("catalog",
      help = "URI for GeoTrellis catalog"
    ).withDefault(new URI("s3://dewberry-demo/testingests"))

    val layerNameOpt = Opts.option[String]("layer",
      help = "Layer name for pyramid")

    val zoomOpt = Opts.option[Int]("zoom",
      help = "Starting zoom level, sample from here")

    val stopOpt = Opts.option[Int]("stop",
      help = "Stop at this zoom level, default 0"
    ).withDefault(0)

    val extentOpt = Opts.option[String]("extent",
      help = "Sub-extent of layer to read and pyramid: 'xmin,ymin,xmax,ymax' in LatLng"
    ).orNone

    val partitionsOpt = Opts.option[Int]("partitions",
      help = "Override number of partitions for reading base layer"
    ).orNone

    ( catalogOpt, layerNameOpt, zoomOpt, stopOpt, extentOpt, partitionsOpt).mapN {
      (catalogUri, layerName, zoom, stop, extent, numPartitions) =>
      println(s"Catalog: $catalogUri")
      println(s"Layer: ${LayerId(layerName, zoom)} @ query: $extent")

      //to solve timeout problem
      System.setProperty("sun.net.client.defaultReadTimeout", "60000")

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

      // using s3 for writing the outputs
      val attributeStore = AttributeStore(catalogUri)

      // Create the writer that we will use to store the tiles in the local catalog.
      val layerId = LayerId(layerName, zoom)
      val reader = LayerReader(attributeStore, catalogUri)
      val writer = LayerWriter(attributeStore, catalogUri)


      val baseLayer = extent match {
        case Some(ex) =>
          val bbox = Extent.fromString(ex).toPolygon
          reader.query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
            .where(Intersects(bbox -> (LatLng: CRS)))
            .result

        case None =>
          reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](layerId)
      }

      require(baseLayer.metadata.crs == WebMercator, s"Layout scheme hard-corded to WebMercator, can't pyramid: ${baseLayer.metadata.crs}")
      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

      Pyramid.upLevels(baseLayer, layoutScheme, zoom, Bilinear) { (rdd, z) =>
          if (z < zoom) { // skip the baseLayer, don't rewrite it
            Util.writeOrUpdateLayer(writer, LayerId(layerName, z), rdd)
          }
      }
    }
  }
)
