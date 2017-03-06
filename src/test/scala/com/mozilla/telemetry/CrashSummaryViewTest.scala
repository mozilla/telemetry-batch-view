package com.mozilla.telemetry

import scala.io.Source
import org.scalatest._
import org.json4s.jackson.JsonMethods._
import org.json4s.Extraction
import org.json4s.DefaultFormats
import com.mozilla.telemetry.views.{CrashSummaryView, CrashSummary, CrashPing}

class CrashSummaryViewTest extends FlatSpec with Matchers {

  def fixture = {
    val crashPingsPath = getClass.getResource("/crash_ping_indented.json").getPath()
    val crashPingsJson = Source.fromFile(crashPingsPath).mkString
    implicit val formats = DefaultFormats
    val crashPingsMap = parse(crashPingsJson)
    new {
      val ping = crashPingsMap.extract[Map[String, Any]]
      val fields = ping.get("fields") match {
        case Some(m: Map[_,_]) => m.asInstanceOf[Map[String,Any]]
        case _  => Map[String, Any]()
      }
      val payload = ping.getOrElse("payload", Map[String, Any]())
      val payloadStr = compact(render(Extraction.decompose(payload)))
    }
  }

  "A well formed ping" should "return a CrashPing" in {
    val maybeCrashPing = CrashSummaryView.transformPayload(
      fixture.fields,
      Some(fixture.payloadStr)
    )
    assert(maybeCrashPing.isDefined)
  }

  "A malformed ping" should "return an undefined option" in {
    val malformedFields = fixture.fields - "geoCountry"
    val maybeCrashPing = CrashSummaryView.transformPayload(
      malformedFields,
      Some(fixture.payloadStr)
    )
    assert(! maybeCrashPing.isDefined)
  }

  "A CrashSummary" can "be created from a CrashPing" in {
    val maybeCrashPing = CrashSummaryView.transformPayload(
      fixture.fields,
      Some(fixture.payloadStr)
    )
    assert(maybeCrashPing.isDefined)
    val crashSummary = maybeCrashPing match {
      case Some(ping: CrashPing) => Some(new CrashSummary(ping))
      case _ => None
    }
    assert(crashSummary.isDefined)
    crashSummary.foreach(x => {
      assert(x.client_id == Some("my-client-id"))
      assert(x.normalized_channel == "beta")
      assert(x.build_version == "40.0")
      assert(x.build_id == "20150706171725")
      assert(x.channel == "beta")
      assert(x.application == "Fennec")
      assert(x.os_name == "Linux")
      assert(x.os_version == "13")
      assert(x.architecture == "arm")
      assert(x.country == "BR")
      assert(x.experiment_id == Some("1"))
      assert(x.experiment_branch == Some("control"))
      assert(x.e10s_cohort == Some("test"))
      assert(x.e10s_enabled == Some(true))
      assert(x.gfx_compositor == Some("opengl"))
      assert(x.payload.processType == Some("main"))
    })
  }

  "CrashSummary" should "support new-style pings" in {
    val submission = fixture.payloadStr
    val fields = fixture.fields + ("submission" -> submission)

    val maybeCrashPing = CrashSummaryView.transformPayload(
      fields,
      None
    )
    assert(maybeCrashPing.isDefined)
    val crashSummary = maybeCrashPing match {
      case Some(ping: CrashPing) => Some(new CrashSummary(ping))
      case _ => None
    }
    assert(crashSummary.isDefined)
    crashSummary.foreach(x => {
      assert(x.client_id == Some("my-client-id"))
      assert(x.normalized_channel == "beta")
      assert(x.build_version == "40.0")
      assert(x.build_id == "20150706171725")
      assert(x.channel == "beta")
      assert(x.application == "Fennec")
      assert(x.os_name == "Linux")
      assert(x.os_version == "13")
      assert(x.architecture == "arm")
      assert(x.country == "BR")
      assert(x.experiment_id == Some("1"))
      assert(x.experiment_branch == Some("control"))
      assert(x.e10s_cohort == Some("test"))
      assert(x.e10s_enabled == Some(true))
      assert(x.gfx_compositor == Some("opengl"))
      assert(x.payload.processType == Some("main"))
    })
  }
}
