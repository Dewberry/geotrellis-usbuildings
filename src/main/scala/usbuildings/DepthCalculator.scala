package usbuildings

import java.net.URI

import cats.implicits._
import com.monovore.decline.{CommandApp, Opts}
import geotrellis.proj4.WebMercator
import geotrellis.raster.MultibandTile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.io.s3.{S3AttributeStore, S3GeoTiffRDD, S3LayerManager, S3LayerWriter, S3Client, AmazonS3Client}
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{FloatingLayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.{LayerId, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.auth._
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._


//import org.apache.spark._
//import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.spark.io._
import geotrellis.spark.{Metadata, _}
import spray.json.DefaultJsonProtocol._


import scala.util.Properties
import geotrellis.raster.split.Split
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.mapalgebra.local.Subtract
import geotrellis.spark.mapalgebra.local._
import geotrellis.spark.io.s3._

// This is an example showing the checker boxes problem with 10m NEDs.
object DepthCalculator extends CommandApp(
  name = "depth",
  header = "",
  main = {
    //val nedOpt = Opts.option[String]("neds", help = "S3 URI prefix of input ned data").withDefault("s3://dewberry-demo/rasters/10m_full_nation")
    //val nedOpt = Opts.option[String]("neds", help = "S3 URI prefix of input ned data").withDefault("s3://dewberry-demo/rasters/10m/AL")
    val wselOpt = Opts.option[String]("wsel", help = "S3 URI prefix of input wsel data").withDefault("s3://dewberry-demo/geotl_prod/wsel_coastal_full_nation")
    val nedOpt = Opts.option[String]("neds", help = "S3 URI prefix of input ned data").withDefault("s3://dewberry-demo/geotl_prod/ned_10m_full_nation_2")
    val outputOpt = Opts.option[String]("output", help = "S3 URI prefix of output tiles").withDefault("s3://dewberry-demo/geotl_prod")
    val layerNameOpt = Opts.option[String]("outputLayerName", help = "Output layer name in the catalog")
    val zoomOpt = Opts.option[String]("maxZoom", help = "Max Zoom depth is calcualted against. This is minumum of two layers resolution(ned vs wsel)")

    ( wselOpt,nedOpt, outputOpt,layerNameOpt,zoomOpt).mapN { (wselUri1, nedUri1, outputUriString,layerName,maxZoom) =>
      val outputUri = new URI(outputUriString)
      if (outputUri.getScheme != "s3") {
        throw new java.lang.IllegalArgumentException("--output must be an S3 URI")
      }
      val bucket = outputUri.getHost
      val path = outputUri.getPath.stripPrefix("/")

      //to solve timeout problem
      System.setProperty("sun.net.client.defaultReadTimeout", "120000")

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

      val wselUri = new URI(wselUri1)
      if (wselUri.getScheme != "s3") {
        throw new java.lang.IllegalArgumentException("--input must be an S3 URI")
      }
      val bucketInpWSEL = wselUri.getHost
      val pathInpWSEL = wselUri.getPath.stripPrefix("/")

      val myArrWSEL = pathInpWSEL.split('/')
      val wselLayerName = myArrWSEL(myArrWSEL.length - 1)


      val nedUri = new URI(nedUri1)
      if (nedUri.getScheme != "s3") {
        throw new java.lang.IllegalArgumentException("--input must be an S3 URI")
      }
      val bucketInpNED = nedUri.getHost
      val pathInpNED = nedUri.getPath.stripPrefix("/")

      val myArrNED = pathInpNED.split('/')
      val nedLayerName = myArrNED(myArrNED.length - 1)

      run(sc)

      def run(implicit sc: SparkContext) = {
        val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)
        //var zoom: Int = 16 //this is now a parameter
        val zoom = maxZoom.toInt

        val getS3ClientCustom: () => S3Client = { () =>
          val config = {
            val config = new com.amazonaws.ClientConfiguration
            config.setMaxConnections(64)
            config.setMaxErrorRetry(16)
            config.setConnectionTimeout(120 * 1000) //ravi added
            config.setSocketTimeout(120*1000) //ravi added
            config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
            // Use AWS SDK default time-out settings before changing
            config
          }
          AmazonS3Client(DefaultAWSCredentialsProviderChain.getInstance(), config)
        }

        //using s3 for writing the outputs
        val attributeStore = S3AttributeStore(bucket, path)

        //val reader: FilteringLayerReader[LayerId] = S3LayerReader("dewberry-demo", "geotl_prod")
        val reader = new S3LayerReader(attributeStore) {
          override def rddReader: S3RDDReader =  new S3RDDReader {
            override def getS3Client: () => S3Client = getS3ClientCustom
          }
        }

        val rddWSEL: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
          reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId(wselLayerName, zoom))

        val rddNED: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] =
          reader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](LayerId(nedLayerName, zoom))

        //rddWSEL.join(rddNED).mapValues { case (tile1: Tile, tile2: Tile) => Subtract(tile1, tile2) }
        val rddout = rddWSEL.withContext(_.join(rddNED).mapValues { case (tile1: MultibandTile, tile2: MultibandTile) => MultibandTile(tile1.bands(0) - (tile2.bands(0)*3.33)) })

        //using s3 for writing the outputs
        //val attributeStore = S3AttributeStore(bucket, path)

        // Create the writer that we will use to store the tiles in the local catalog.
        val writer = S3LayerWriter(attributeStore)

        // Pyramiding up the zoom levels, write our tiles out to the local file system.
        Pyramid.upLevels(rddout, layoutScheme, zoom, Bilinear) { (rdd, z) =>
          val layerId = LayerId(layerName, z)
          //val layerId = LayerId("depth_coastal_10m_full_nation", z)
          //val layerId = LayerId("dem_10m_" + stateName, z)
          //val layerId = LayerId("dem_10m_full_nation_", z)
          if (z == 0) {
            val histogram = rddout.histogram
            attributeStore.write(layerId, "histogram", histogram)
          }
          // If the layer exists already, delete it out before writing
          if (attributeStore.layerExists(layerId)) {
            new S3LayerManager(attributeStore).delete(layerId)
          }
          writer.write(layerId, rdd, ZCurveKeyIndexMethod)
        }
      }
    }
  }
)
