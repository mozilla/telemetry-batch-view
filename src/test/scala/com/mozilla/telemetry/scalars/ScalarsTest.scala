import com.mozilla.telemetry.metrics.ScalarsClass

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class ScalarsTest extends FlatSpec with Matchers {

  val fixture = {
    new {
      val scalars = new ScalarsClass{
        override protected val getURL = (a: String, b: String) => Source.fromFile("src/test/resources/Scalars.yaml")
      }

      private val optStatuses = List(true, false)

      val names = optStatuses map {
        optStatus => 
            optStatus -> scalars.definitions(optStatus).map{
              case (k, value) => 
                k.toLowerCase
            }
      } toMap
    }
  }

  "Optout scalars" must "be included when optout only" in {
    val scalars = fixture.names(false)
    assert(scalars.exists(_ == "mock.uint.optout"))
  }

  "Optin scalars" must "not be included when optout only" in {
    val scalars = fixture.names(false)
    assert(!scalars.exists(_ == "mock.uint.optin"))
  }

  "Optout scalars" must "be included when optin and optout" in {
    val scalars = fixture.names(true)
    assert(scalars.exists(_ == "mock.uint.optout"))
  }

  "Optin scalars" must "be included when optin and optout" in {
    val scalars = fixture.names(true)
    assert(scalars.exists(_ == "mock.uint.optin"))
  }
}
