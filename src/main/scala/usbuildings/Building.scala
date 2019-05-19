package usbuildings

import java.io.FileNotFoundException
import java.net.URL
import java.security.InvalidParameterException

import com.typesafe.scalalogging.LazyLogging
import geotrellis.raster.histogram.StreamingHistogram
import geotrellis.vector._
import geotrellis.vectortile

import scala.collection.JavaConverters._

/** Id for building reocrd, based on its position source file */
case class Id(file: String, idx: Int)

/** Container class to work with Building data */
case class Building(
  id: Id,
  footprint: Polygon,
  histograms: Map[String, Option[StreamingHistogram]] = Map.empty,
  errors: List[String] = Nil
) {
  def toVectorTileFeature: Feature[Polygon, Map[String, vectortile.Value]] = {
    val histAttributes: Map[String, vectortile.Value] = {
      histograms.flatMap({ case (name, h) =>
        val minMax: Option[(Double, Double)] = h.flatMap(_.minMaxValues())
        minMax match {
          case Some((min, max)) =>
            List(
              s"${name}_min" -> vectortile.VDouble(min),
              s"${name}_max" -> vectortile.VDouble(max))
          case None => Nil
        }
      })
    }
    val errAttributes = Map("errors" -> vectortile.VString(errors.mkString(", ")))
    Feature(footprint, errAttributes ++ histAttributes)
  }

  def withError(err: String): Building = {
    copy(errors = err :: errors)
  }
}

object Building extends LazyLogging {
  import java.io.{BufferedReader, InputStream, InputStreamReader}
  import java.util.zip.ZipInputStream

  import geotrellis.vector.Polygon
  import geotrellis.vector.io._

  // Greedy match, will trim white space around but won't ensure proper GeoJSON
  val FeatureRx = """.*(\{\"type\":\"Feature\".+}).*""".r

  /**
    * California.geojson is 2.66GB uncompressed, need to read it as a stream to avoid blowing the heap\
    * Supports: .zip, .json, .geojson files
  */
  def readFromGeoJson(url: URL): Iterator[Building] = {
    // TODO: consider how bad it is to leave the InputStream open
    // TODO: consider using is.seek(n) to partition reading the list
    val is: InputStream = url.getPath match {
      case null =>
        throw new FileNotFoundException("Can't read")

      case p if p.endsWith(".geojson") || p.endsWith(".json") =>
        url.openStream()

      case p if p.endsWith(".zip") =>
        val zip = new ZipInputStream(url.openStream)
        val entry = zip.getNextEntry
        logger.info(s"Reading: $url - ${entry.getName}")
        zip

      case _ =>
        throw new InvalidParameterException(s"Can't read: $url format")
    }

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(is))
    val stream = reader.lines()
    var idx: Int = 0
    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val poly = json.parseGeoJson[Polygon]
        idx += 1
        if (poly.isValid)
          Some(Building(Id(url.getFile, idx), poly))
        else {
          logger.warn(s"Dropping invalid geometry: ${poly.toWKT}")
          None
        }
      case _ =>
        None
    }
  }

  def geoJsonURLs: List[String] = List(
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Alabama.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Alaska.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Arizona.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Arkansas.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/California.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Colorado.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Connecticut.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Delaware.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/DistrictofColumbia.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Florida.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Georgia.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Hawaii.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Idaho.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Illinois.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Indiana.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Iowa.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Kansas.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Kentucky.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Louisiana.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Maine.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Maryland.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Massachusetts.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Michigan.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Minnesota.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Mississippi.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Missouri.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Montana.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Nebraska.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Nevada.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NewHampshire.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NewJersey.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NewMexico.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NewYork.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NorthCarolina.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/NorthDakota.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Ohio.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Oklahoma.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Oregon.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Pennsylvania.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/RhodeIsland.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/SouthCarolina.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/SouthDakota.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Tennessee.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Texas.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Utah.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Vermont.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Virginia.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Washington.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/WestVirginia.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Wisconsin.zip",
    "http://s3.amazonaws.com/dewberry-demo/bing-us-buildings-zips/Wyoming.zip")
}
