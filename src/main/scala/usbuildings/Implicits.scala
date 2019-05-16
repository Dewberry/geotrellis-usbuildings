package usbuildings

import cats._
import geotrellis.raster.histogram._

object Implicits {
/** Instance of interface that defines how to:
  * - get an empty/initial value of StreamingHistogram
  * - combine two instances of StreamingHistogram
  *
  * This allows using polygonalSummary methods without providing initial value like:
  *
  * {{{
  * val summary = raster.polygonalSummary(geometry)
  * }}}
  *
  * @see https://typelevel.org/cats/typeclasses/semigroup.html
  */
  implicit val streamingHistogramSmigroup: Semigroup[StreamingHistogram] =
    new Semigroup[StreamingHistogram] {
      def combine(x: StreamingHistogram, y: StreamingHistogram): StreamingHistogram = x.merge(y)
    }
}