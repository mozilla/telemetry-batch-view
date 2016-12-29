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
      case Some(ping: CrashPing) => new CrashSummary(ping)
      case _ => None
    }
    assert(crashSummary != None)
  }
}
