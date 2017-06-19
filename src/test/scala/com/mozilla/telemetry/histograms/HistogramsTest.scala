import com.mozilla.telemetry.metrics.HistogramsClass

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import com.mozilla.telemetry.utils.MainPing

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

      val definitions = optStatuses map {
        optStatus => optStatus -> histograms.definitions(optStatus)
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

  "Histograms" must "have processes specified" in {
    val histograms = fixture.definitions(true)
    assert(histograms("MOCK_KEYED_EXPONENTIAL").processes == MainPing.ProcessTypes)
  }

  "Histograms all_child process" must "have child processes specified" in {
    val histograms = fixture.definitions(true)
    assert(histograms("CHILD_PROCESSES_ONLY").processes == MainPing.ProcessTypes.filter(_ != "parent"))
  }
}
