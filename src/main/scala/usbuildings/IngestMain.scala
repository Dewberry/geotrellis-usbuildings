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
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.auth._
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.model._
import spray.json.DefaultJsonProtocol._
import scala.util.{Properties, Try, Success, Failure}


// This is an example showing the checker boxes problem with 10m NEDs.
object IngestMain extends CommandApp(
  name = "update-ingest",
  header = "",
  main = {
    val inputOpt = Opts.option[java.net.URI]("input", help = "S3 URI prefix of input rasters")
      .withDefault(new URI("s3://dewberry-demo/rasters/10m_region4"))

    val outputOpt = Opts.option[URI]("output", help = "S3 URI prefix of output catalog")
      .withDefault(new URI("s3://dewberry-demo/testingests"))

    val layerNameOpt = Opts.option[String]("layer", help = "Layer name in above catalog")

    val histogramOpt = Opts.flag("histogram", help = "Caluclate histogram on ingest").orFalse

    ( inputOpt, outputOpt, layerNameOpt, histogramOpt).mapN { (inputUri, outputUri, layerName, histogram) =>
      println(s"Input: $inputUri")
      println(s"Catalog: $outputUri")
      println(s"Layer: $layerName")

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

      val bucketInp = inputUri.getHost
      val pathInp = inputUri.getPath.stripPrefix("/")
      val myArr = pathInp.split('/')
      val stateName = myArr(myArr.length - 1)

      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

      /** More optimal COG ingest that does not yet deal well with Striped GeoTiff segments */
      // val paths: List[String] = {
      //   val s3Client = Util.getS3Client
      //   s3Client
      //     .listKeys(bucketInp, pathInp)
      //     .toList
      //     .map { key => s"s3://$bucketInp/$key" }
      // }
      // paths.foreach(p => println(s"Read: $p"))
      // val sourceRDD: RDD[RasterSource] =
      //   sc.parallelize(paths, paths.length)
      //     .map({ uri => GDALRasterSource(uri).reproject(WebMercator, Bilinear): RasterSource })
      //     .cache()
      // val summary = RasterSummary.fromRDD[RasterSource, Long](sourceRDD)
      // val LayoutLevel(zoom, layout) = summary.levelFor(layoutScheme)
      // println(s"Zoom: $zoom")
      // val reprojected: MultibandTileLayerRDD[SpatialKey] =
      //   RasterSourceRDD.tiledLayerRDD(sourceRDD, layout,
      //     rasterSummary = summary.some,
      //     partitioner = Some(new HashPartitioner(summary.estimatePartitionsNumber * 2)))

      val inputRDD: RDD[(ProjectedExtent, MultibandTile)] = {
        val getS3Client: () => S3Client = { () =>
          val config = {
            val config = new com.amazonaws.ClientConfiguration
            config.setMaxConnections(64)
            config.setMaxErrorRetry(16)
            config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
            // Use AWS SDK default time-out settings before changing
            config
          }
          AmazonS3Client(DefaultAWSCredentialsProviderChain.getInstance(), config)
        }
        val options = S3GeoTiffRDD.Options.DEFAULT.copy(getS3Client = getS3Client)
        S3GeoTiffRDD.spatialMultiband(bucketInp, pathInp, options)
      }

      // instead of using TileLayerMetadata.fromRDD gather metadata directly from files
      // there is a risk here that we're not capturing the same files as S3GeoTiffRDD
      // however we avoid reading in geotiff segments just to read their metadata
      val rasterSummary = {
        val paths: List[String] =
          Util.getS3Client.listKeys(bucketInp, pathInp).map({ key => s"s3://$bucketInp/$key" }).toList
        paths.foreach(p => println(s"Read Metadata: $p"))
        val sourceRDD: RDD[RasterSource] =
          sc.parallelize(paths, paths.length)
            .map({ uri => GeoTiffRasterSource(uri): RasterSource })
            .cache()
        RasterSummary.fromRDD[RasterSource, Long](sourceRDD)
      }
      println(s"RasterSummary: $rasterSummary")

      val (zoom, reprojected: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) = {
        // val (_, metadata) = TileLayerMetadata.fromRDD(inputRDD, FloatingLayoutScheme(512))
        val metadata: TileLayerMetadata[SpatialKey] = {
          val level = rasterSummary.levelFor(FloatingLayoutScheme(512))
          rasterSummary.toTileLayerMetadata(level)._1 // discard zoom level
        }
        val inputTiledRDD = inputRDD.tileToLayout(metadata.cellType, metadata.layout, Bilinear)
        val (z, reprojected1) = MultibandTileLayerRDD(inputTiledRDD, metadata).reproject(WebMercator, layoutScheme, Bilinear)
        (z, reprojected1)
      }

      // using s3 for writing the outputs
      val attributeStore = AttributeStore(outputUri)

      // Create the writer that we will use to store the tiles in the local catalog.
      val writer = LayerWriter(attributeStore, outputUri)

      /** Write or udpate a layer, creates layer with bounds potentially larger than rdd
       * @param layerExtent Maximum extent for all likely updates to layer in LatLng
       * @param id LayerId to be create
       * @param rdd Tiles for initial or udpate write
       */
      def writeOrUpdate(layerExtent: Extent, id: LayerId, rdd: MultibandTileLayerRDD[SpatialKey]): Unit = {
        if (attributeStore.layerExists(id)) {
          println(s"Updating: $id")
          writer.update(id, rdd)
        } else {
          val maxExtent = layerExtent.reproject(LatLng, rdd.metadata.crs)
          val maxBounds = KeyBounds(rdd.metadata.layout.mapTransform.extentToBounds(maxExtent))
          val keyIndex: KeyIndex[SpatialKey] = ZCurveKeyIndexMethod.createIndex(maxBounds)
          println(s"Writing: $id")
          writer.write(id, rdd, keyIndex)
        }
      }

      val conusExtent = Extent(-127, 19,-66, 52)
      // write base layer to disk to branch for two jobs, calculate histogram and write
      reprojected.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val topLayerId = LayerId(layerName, 0)

      if (histogram) {
        Try(attributeStore.read[Array[Histogram[Double]]](topLayerId, "histogram")) match {
          case Success(savedHistograms) =>
            val updateHistograms = reprojected.histogram()
            val mergedHistograms: Array[Histogram[Double]] =
              for { (sh, uh) <- savedHistograms.zip(updateHistograms) }
              yield (new StreamingHistogram(sh.bucketCount)).merge(sh).merge(uh): Histogram[Double]

            val oldMinMax = savedHistograms.map(_.minMaxValues())
            val updMinMax = updateHistograms.map(_.minMaxValues())
            val mrgMinMax = mergedHistograms.map(_.minMaxValues())
            println(s"Updating histogram for $topLayerId, old: $oldMinMax upd: $updMinMax mrg: $mrgMinMax")
            attributeStore.write(topLayerId, "histogram", mergedHistograms)

          case Failure(e: AttributeNotFoundError) =>
            val updateHistograms = reprojected.histogram()
            println(s"Saving histogram for $topLayerId, new: ${updateHistograms.map(_.minMaxValues())}")
            attributeStore.write(topLayerId, "histogram", updateHistograms)

          case Failure(e) =>
            sys.error(s"Failed to read saved histogram: $e")
        }
      }

      // Pyramiding up the zoom levels, write our tiles out to the local file system.
      Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
        val layerId = LayerId(layerName, z)
        writeOrUpdate(conusExtent, layerId, rdd)
      }
    }
  }
)
