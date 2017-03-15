import com.mozilla.telemetry.histograms.HistogramsClass

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class HistogramsTest extends FlatSpec with Matchers {

  val fixture = {
    new {
      val histograms = new HistogramsClass{
        override protected val getURL = (a: String, b: String) => Source.fromFile("src/test/resources/ShortHistograms.json")
      }

      private val optStatuses = List(false, true)

      val names = optStatuses map {
        optStatus =>
            optStatus -> histograms.definitions(optStatus).map{
              case (k, value) =>
                k.toLowerCase
            }
      } toMap
    }
  }

  "Optout histograms" must "be included when optout only" in {
    val histograms = fixture.names(false)
    assert(histograms.exists(_ == "mock_optout"))
  }

  "Optin histograms" must "not be included when optout only" in {
    val histograms = fixture.names(false)
    assert(!histograms.exists(_ == "mock_optin"))
  }

  "Optout histograms" must "be included when optin and optout" in {
    val histograms = fixture.names(true)
    assert(histograms.exists(_ == "mock_optout"))
  }

  "Optin histograms" must "be included when optin and optout" in {
    val histograms = fixture.names(true)
    assert(histograms.exists(_ == "mock_optin"))
  }
}
