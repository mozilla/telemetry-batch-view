import com.mozilla.telemetry.histograms.HistogramsClass

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

class HistogramsTest extends FlatSpec with Matchers {

  val fixture = {
    new {
      val histograms = new HistogramsClass{
        override protected val getURL = (a: String, b: String) => Source.fromFile("src/test/resources/Histograms.json")
      }

      private val processes = List(Some("parent"), Some("content"), Some("gpu"), None)
      private val optStatuses = List(true, false)

      private val combinations = for {
        process <- processes
        optStatus <- optStatuses
      } yield (optStatus, process)

      val names = combinations map {
        case (optStatus, process) => 
            (optStatus, process) -> histograms.definitions(optStatus, process).map{
              case (k, value) => 
                k.toLowerCase
            }
      } toMap
    }
  }

  "All histograms" must "be included when no process specified" in {
    val histograms = fixture.names((false, None))

    assert(histograms.exists(_ == "fips_enabled"))
    assert(histograms.exists(_ == "fips_enabled_content"))
    assert(histograms.exists(_ == "fips_enabled_gpu"))
  }

  "Parent histograms" must "only be included when parent specified" in {
    val histograms = fixture.names((false, Some("parent")))

    assert(histograms.exists(_ == "fips_enabled"))
    assert(!histograms.exists(_ == "fips_enabled_content"))
    assert(!histograms.exists(_ == "fips_enabled_gpu"))
  }

  "Content histograms" must "only be included when content specified" in {
    val histograms = fixture.names((false, Some("content") ))

    assert(!histograms.exists(_ == "fips_enabled"))
    assert(histograms.exists(_ == "fips_enabled_content"))
    assert(!histograms.exists(_ == "fips_enabled_gpu"))
  }

  "Gpu histograms" must "only be included when gpu specified" in {
    val histograms = fixture.names((false, Some("gpu")))

    assert(!histograms.exists(_ == "fips_enabled"))
    assert(!histograms.exists(_ == "fips_enabled_content"))
    assert(histograms.exists(_ == "fips_enabled_gpu"))
  }

  "Optout histograms" must "be included when optout" in {
    val histograms = fixture.names((false, None))
    assert(histograms.exists(_ == "fips_enabled"))
  }

  "Optin histograms" must "not be included when optout" in {
    val histograms = fixture.names((false, None))
    assert(!histograms.exists(_ == "gc_ms"))
  }

  "Optout histograms" must "be included when optin" in {
    val histograms = fixture.names((true, None))
    assert(!histograms.exists(_ == "fips_enabled"))
  }

  "Optin histograms" must "be included when optin" in {
    val histograms = fixture.names((true, None))
    assert(histograms.exists(_ == "gc_ms"))
  }
}
