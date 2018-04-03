import com.mozilla.telemetry.metrics.ScalarsClass

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import com.mozilla.telemetry.utils.MainPing

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

      val definitions = optStatuses map {
        optStatus => optStatus -> scalars.definitions(optStatus)
      } toMap
    }
  }

  "Optout scalars" must "be included when optout only" in {
    val scalars = fixture.names(false)
    assert(scalars.exists(_ == "scalar_parent_mock_uint_optout"))
  }

  "Optin scalars" must "not be included when optout only" in {
    val scalars = fixture.names(false)
    assert(!scalars.exists(_ == "scalar_praent_mock_uint_optin"))
  }

  "Optout scalars" must "be included when optin and optout" in {
    val scalars = fixture.names(true)
    assert(scalars.exists(_ == "scalar_parent_mock_uint_optout"))
  }

  "Optin scalars" must "be included when optin and optout" in {
    val scalars = fixture.names(true)
    assert(scalars.exists(_ == "scalar_parent_mock_uint_optin"))
  }

  "Scalars" must "have processes specified" in {
    val scalars = fixture.definitions(true)
    assert(scalars.contains("scalar_parent_mock_uint_optin"))
    assert(scalars.contains("scalar_gpu_mock_uint_optin"))
    assert(scalars.contains("scalar_content_mock_uint_optin"))
  }

  "Scalars all_child process" must "have child processes specified" in {
    val scalars = fixture.definitions(true)
    assert(!scalars.contains("scalar_parent_mock_all_children"))
    assert(scalars.contains("scalar_gpu_mock_all_children"))
    assert(scalars.contains("scalar_content_mock_all_children"))
  }

  "Scalars process" must "match name" in {
    val scalars = fixture.definitions(true)
    assert(scalars("scalar_parent_mock_uint_optin").process.get == "parent")
    assert(scalars("scalar_gpu_mock_uint_optin").process.get == "gpu")
    assert(scalars("scalar_content_mock_uint_optin").process.get == "content")
  }

  "Scalars originalName" must "be correct" in {
    val scalars = fixture.definitions(true)
    assert(scalars("scalar_parent_mock_uint_optin").originalName == "mock.uint.optin")
  }

  "Addon scalars" must "be included" in {
    val scalars = fixture.definitions(true)
    assert(scalars("telemetry_mock_string_kind").originalName == "telemetry.mock.string_kind")
    assert(scalars("telemetry_mock_string_kind").process.get == "dynamic")
  }
}
