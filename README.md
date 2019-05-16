# GeoTrellis Polygonal Summary Demo

This project demonstrates generating polygonal summary for a feature data set over high resolution raster.

## Inputs

Features used are building footprints from: https://github.com/Microsoft/USBuildingFootprints

> This dataset contains 125,192,184 computer generated building footprints in all 50 US states.

These features are read directly from zipped GeoJSON files.

The rasters used are 1/3 arc-second NED GeoTiffs hosted at: `s3://azavea-datahub/raw/ned-13arcsec-geotiff/`
The files are gridded and named by their northing and easting degree (ex: `imgn36w092_13.tif`).

The rasters are sampled directly from that location.

## Output

Output is a vector tile layer that includes the min and max elevation under each geometry.

## Inventory

[`build.sbt`](build.sbt): Scala Build Tool build configuration file
[`.sbtopts`](.sbtopts): Command line options for SBT, including JVM parameters
[`project`](project): Additional configuration for SBT project, plugins, utility, versions
[`src/main/scala`](src/main/scala): Application and utility code

## Spark Job Commands

### Local

```
sbt:geotrellis-usbuildings> test:runMain usbuildings.BuildingSummaryMain --buildings https://usbuildingdata.blob.core.windows.net/usbuildings-v1-1/RhodeIsland.zip --output file:/tmp/usbuildings-ri-v01 --layer wsel_riverine,s3://dewberry-demo/geotl_prod,wsel_riverine_approx_full_nation,16 --layer depth,s3://dewberry-demo/geotl_prod,depth_riverine_10m_full_nation,14
```

### EMR

At minimum, you'll need to change the following sbt lighter configuration variables in `build.sbt`
to resources within your AWS account before starting the EMR cluster:
- `sparkS3JarFolder`: Must be an S3 path within the AWS account that the cluster can read/write to
- `sparkS3LogUri`: Must be an S3 path within the AWS account that the cluster can read/write to
- `sparkJobFlowInstancesConfig`: Must point to the EC2 key name you have access to

Be sure to `reload` in sbt after making changes to `build.sbt`.

When running the commands below, be sure you replace the S3 url for the `--output` parameter
to point to a valid S3 path that the EMR cluster has permissions to write to.

```
sbt:geotrellis-usbuildings> sparkSubmitMain usbuildings.BuildingSummaryMain --all-buildings --buildings foo --output s3://bucket/path/prefix --layer wsel_riverine,s3://dewberry-demo/geotl_prod,wsel_riverine_approx_full_nation,16 --layer depth,s3://dewberry-demo/geotl_prod,depth_riverine_10m_full_nation,14
```
