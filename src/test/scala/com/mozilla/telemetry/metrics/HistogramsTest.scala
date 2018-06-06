/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
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
        optStatus => optStatus -> histograms.definitions(optStatus).map{
          case (k, value) => (k.toLowerCase, value)
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

  "Histograms" must "have processes specified" in {
    val histograms = fixture.definitions(true)
    assert(histograms.contains("mock_keyed_exponential"))
    assert(histograms.contains("mock_keyed_exponential_content"))
    assert(histograms.contains("mock_keyed_exponential_gpu"))
  }

  "Histograms all_child process" must "have child processes specified" in {
    val histograms = fixture.definitions(true)
    assert(!histograms.contains("child_processes_only"))
    assert(histograms.contains("child_processes_only_gpu"))
    assert(histograms.contains("child_processes_only_content"))
  }

  "Histograms process" must "match name" in {
    val histograms = fixture.definitions(true)
    assert(histograms("mock_keyed_exponential").process.get == "parent")
    assert(histograms("mock_keyed_exponential_content").process.get == "content")
    assert(histograms("mock_keyed_exponential_gpu").process.get == "gpu")
  }

  "Histograms originalName" must "be correct" in {
    val histograms = fixture.definitions(true)
    assert(histograms("mock_keyed_exponential").originalName == "MOCK_KEYED_EXPONENTIAL")
  }
}
