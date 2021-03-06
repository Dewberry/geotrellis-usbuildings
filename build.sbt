import Dependencies._

name := "geotrellis-calTest"

scalaVersion := Version.scala
scalaVersion in ThisBuild := Version.scala

licenses := Seq(
  "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-language:experimental.macros",
  "-Ypartial-unification" // Required by Cats
)
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ =>
  false
}
addCompilerPlugin(
  "org.spire-math" % "kind-projector" % "0.9.4" cross CrossVersion.binary)
addCompilerPlugin(
  "org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full)
dependencyUpdatesFilter := moduleFilter(organization = "org.scala-lang")
resolvers ++= Seq(
  "geosolutions" at "http://maven.geo-solutions.it/",
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "osgeo" at "http://download.osgeo.org/webdav/geotools/",
  Resolver.bintrayRepo("azavea", "geotrellis")
)

libraryDependencies ++= Seq(
  sparkCore % Provided,
  sparkSQL % Provided,
  sparkHive % Provided,
  geotrellisSpark,
  geotrellisS3,
  geotrellisShapefile,
  geotrellisGeotools,
  geotrellisVectorTile,
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm"  % "2.11.0",
  "com.azavea.geotrellis" %% "geotrellis-contrib-gdal"  % "2.11.0",
  "org.geotools" % "gt-ogr-bridj" % Version.geotools
    exclude ("com.nativelibs4java", "bridj"),
  "com.nativelibs4java" % "bridj" % "0.6.1",
  "com.monovore" %% "decline" % "0.5.1"
)

// auto imports for local dev console
initialCommands in console :=
  """
import usbuildings._
import java.net._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.gdal._
"""

// settings for local testing
Test / fork := true
Test / parallelExecution := false
Test / testOptions += Tests.Argument("-oD")
Test / javaOptions ++= Seq("-Xms1024m",
  "-Xmx8144m",
  "-Djava.library.path=/usr/local/lib")

// Settings for sbt-assembly plugin which builds fat jars for spark-submit
assemblyMergeStrategy in assembly := {
  case "reference.conf"   => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      // Concatenate these to keep JAI happy.
      case ("javax.media.jai.registryFile.jai" :: Nil) |
           ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(
          ".SF"))
          MergeStrategy.discard
        else
          MergeStrategy.first
      }
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._

sparkEmrRelease := "emr-5.23.0"
sparkAwsRegion := "us-east-1"
sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia")
sparkEmrBootstrap := List(BootstrapAction("Install GDAL + dependencies",
  "s3://geotrellis-test/usbuildings/bootstrap.sh",
  "s3://geotrellis-test/usbuildings",
  "v1.0"))
//Add job titile
sparkS3JarFolder := "s3://dewberry-demo/bats/geotrellis/usbuildings/jars"
sparkInstanceCount := 100
sparkMasterType := "i3.4xlarge"
sparkCoreType := "i3.4xlarge"
sparkMasterPrice := Some(0.5)
sparkCorePrice := Some(0.5)
sparkCoreEbsSize := Some(800)
sparkMasterEbsSize := Some(200)

//Cluster name
sparkClusterName := s"geotrellis-usbuildings - ${sys.env.getOrElse("USER", "<anonymous user>")}"
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName(
  "AzaveaKeyPair")
sparkS3LogUri := Some("s3://dewberry-demo/bats/geotrellis/calTest/logs")
sparkEmrConfigs := List(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"
  ),
  EmrConfig("spark-defaults").withProperties(
    "spark.driver.maxResultSize" -> "3G",
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.shuffle.service.enabled" -> "true",
    "spark.shuffle.compress" -> "true",
    "spark.shuffle.spill.compress" -> "true",
    "spark.dynamicAllocation.executorIdleTimeout" -> "1200", //ravi adding this custom because executors are timing out.
    //Partition scheme (test1 = 1280)
    "spark.default.parallelism" -> "960", //Testing 960 partitions for 106 Grids at ~2GB/grid slawler
    "spark.rdd.compress" -> "true",
    "spark.driver.extraJavaOptions" -> "-Djava.library.path=/usr/local/lib",
    "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -Djava.library.path=/usr/local/lib",
    "spark.executorEnv.LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("spark-env").withProperties(
    "LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)

//default # cores = 320 for 20 m5.2xlarge

//Init Cluster command: sparkSubmitMain usbuildings.Main