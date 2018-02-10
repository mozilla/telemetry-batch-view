package com.mozilla.telemetry.views

import org.apache.spark.sql.functions.{col, collect_list, udf}
import com.mozilla.telemetry.utils.getOrCreateSparkSession
import com.mozilla.telemetry.views.ExperimentAnalysisView.Conf
import com.mozilla.telemetry.views.ExperimentSummaryView.jobName
import org.apache.spark.sql.{Dataset, Row}
import org.rogach.scallop.ScallopConf



object PioneerDwellTimeView {
  case class LogEvent(timestamp: Long, details: String, url: String, document_id: String) {
    def getDomain: String = {
      // We're wrapping this in an Option AND Try because java.net.URI can return a null and throw an exception
      val hostString = scala.util.Try(Option(new java.net.URI(url).getHost)) match {
        case scala.util.Success(Some(u)) => u
        // We've seen some urls with unencoded characters cause exceptions -- try just grabbing front of url
        case _ => scala.util.Try(Option(new java.net.URI(url.split('/').take(3).mkString("/")).getHost)) match {
          case scala.util.Success(Some(u)) => u
          case _ => url.split('/')(2)
        }
      }
      val splitString = hostString.split('.') // if you use a string instead of char it'll be interpreted as a regex
      splitString.takeRight(2).mkString(".")
    }
  }

  case class GroupedEvents(pioneer_id: String, entries: Array[Array[LogEvent]])

  case class DwellTime(pioneer_id: String, document_ids: List[String], visit_start_date: java.sql.Date, domain: String,
                       visit_start_time: Long, total_dwell_time: Long, total_idle_time: Long, nav_event_count: Int,
                       log_events: List[LogEvent])

  class Visit(pioneerId: String, initial_event: LogEvent) extends java.io.Serializable {
    var documentIds = Set(initial_event.document_id)
    val domain = initial_event.getDomain
    val visitStartTime = initial_event.timestamp
    var totalDwellTime = 0L
    var totalIdleTime = 0L
    var events = Array(initial_event)

    def visitStartDate: java.sql.Date = new java.sql.Date(visitStartTime * 1000)

    def addActiveTime(event: LogEvent) = {
      documentIds += event.document_id
      totalDwellTime += (event.timestamp - events.last.timestamp)
      events :+= event
    }

    def addIdleTime(event: LogEvent) = {
      val delta = event.timestamp - events.last.timestamp
      documentIds += event.document_id
      totalDwellTime += delta
      totalIdleTime += delta
      events :+= event
    }

    def asDwellTime: DwellTime = {
      val (navEvents, _) = events.foldLeft((0, "")){
        case ((navs: Int, prev: String), e: LogEvent) => (navs + (if (prev == e.url) 0 else 1), e.url)
      }
      DwellTime(pioneerId, documentIds.toList, visitStartDate, domain, visitStartTime, totalDwellTime, totalIdleTime,
        navEvents, events.toList)
    }
  }

  class DwellTimeStateMachine(pioneer_id: String, event: LogEvent) extends java.io.Serializable {
    sealed trait DwellTimeState
    case object VisitActive extends DwellTimeState
    case object VisitIdle extends DwellTimeState
    case object NotInVisit extends DwellTimeState

    sealed trait UrlState
    case object SameUrl extends UrlState
    case object SameDomain extends UrlState
    case object DifferentDomain extends UrlState

    val idleThreshold = 30 * 60 // 30 minutes idle = new visit
    private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

    var currentVisit: Visit = new Visit(pioneer_id, event)
    var lastEvent: LogEvent = event
    var currentState: DwellTimeState = VisitActive
    var dwellTimes: Array[DwellTime] = Array()
    var allDocumentIds: Set[String] = Set()

    def addEvent(event: LogEvent): Unit = {
      val event_details = event.details
      val url_state = compareUrls(lastEvent, event)
      val delta = event.timestamp - lastEvent.timestamp

      currentState match {
        case VisitActive =>
          (event_details, url_state, delta) match {
            case ("focus-start", SameDomain, _) =>
              currentVisit.addActiveTime(event)
            case ("focus-start", DifferentDomain, _) =>
              endCurrentVisitAndActivateNew(event)
            case ("idle-start", SameUrl, _) =>
              currentVisit.addActiveTime(event)
              currentState = VisitIdle
            case ("focus-end", SameUrl, _) =>
              currentVisit.addActiveTime(event)
              endCurrentVisit
            case _ =>
              logger.warn(s"Unexpected state addition: $event to current state $currentState. url_state: $url_state, delta: $delta")
              // If this is a brand new document Id, perhaps we're missing a ping. We attempt to recover
              if (!allDocumentIds.contains(event.document_id)) {
                endCurrentVisitAndActivateNew(event)
              }
          }
        case VisitIdle =>
          (event_details, url_state, delta) match {
            case _ if delta > idleThreshold && (event_details != "idle-start") =>
              endCurrentVisitAndActivateNew(event)
            case ("idle-start", _, _) if delta > idleThreshold =>
              endCurrentVisit
            case ("focus-start", DifferentDomain, _) =>
              currentVisit.addIdleTime(event)
              endCurrentVisitAndActivateNew(event)
            case ("focus-start", _, _) =>
              activateIdleVisit(event)
            case ("idle-end", SameUrl, _) =>
              activateIdleVisit(event)
            case ("focus-end", SameUrl, _) =>
              currentVisit.addIdleTime(event)
              endCurrentVisit
            case _ =>
              logger.warn(s"Unexpected state addition: $event to current state $currentState. url_state: $url_state, delta: $delta")
              // If this is a brand new document Id, perhaps we're missing a ping. We attempt to recover
              if (!allDocumentIds.contains(event.document_id)) {
                endCurrentVisitAndActivateNew(event)
              }
          }
        case NotInVisit =>
          (event_details, url_state, delta) match {
            case ("focus-start", _, _) =>
              newVisit(event)
            case _ =>
              logger.warn(s"Unexpected state addition: $event to current state $currentState. url_state: $url_state, delta: $delta")
              // If this is a brand new document Id, perhaps we're missing a ping. We attempt to recover
              if (!allDocumentIds.contains(event.document_id)) {
                newVisit(event)
              }
          }
      }

      allDocumentIds += event.document_id
      lastEvent = event
    }

    def compareUrls(oldEvent: LogEvent, newEvent: LogEvent): UrlState = {
      if (oldEvent.url == newEvent.url) SameUrl
      else if (oldEvent.getDomain == newEvent.getDomain) SameDomain
      else DifferentDomain
    }

    def endCurrentVisitAndActivateNew(event: LogEvent) = {
      dwellTimes :+= currentVisit.asDwellTime
      currentVisit = new Visit(pioneer_id, event)
      currentState = VisitActive
    }

    def endCurrentVisit = {
      dwellTimes :+= currentVisit.asDwellTime
      currentState = NotInVisit
    }

    def activateIdleVisit(event: LogEvent, addNav: Boolean = false) = {
      currentVisit.addIdleTime(event)
      currentState = VisitActive
    }

    def newVisit(event: LogEvent) = {
      currentVisit = new Visit(pioneer_id, event)
      currentState = VisitActive
    }

    def end: Array[DwellTime] = {
      currentState match {
        case NotInVisit => dwellTimes
        case _ => dwellTimes :+ currentVisit.asDwellTime
      }
    }
  }

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for pioneer online news data", required = true)
    val outputPath = opt[String]("output", descr = "Output path for parquet data", required = true)
    verify()
  }

  def main(args: Array[String]) {
    val spark = getOrCreateSparkSession(jobName)
    val conf = new Conf(args)
    import spark.implicits._
    val actions = List("focus-start", "focus-end", "idle-start", "idle-end")

    val addDocId = udf((docId: String, logs: Seq[Row]) =>
      logs.map(r => LogEvent(r.getLong(0), r.getString(1), r.getString(2), docId)))

    val online_news_entries = spark.read.parquet(s"s3://${conf.inputBucket()}/pioneer-online-news-log-parquet/v1/")

    val flattenedEvents = online_news_entries
      .select(col("metadata.pioneer_id").alias("pioneer_id"), addDocId(col("metadata.document_id"), col("entries")).alias("entries"))
      .groupBy("pioneer_id")
      .agg(collect_list("entries").alias("entries"))
      .as[GroupedEvents]
    val dwell_times: Dataset[DwellTime] = flattenedEvents.flatMap { pioneer: GroupedEvents =>
      try {
        val entries = pioneer.entries.flatten.filter(e => actions.contains(e.details)).sortBy(_.timestamp)
        (entries.length > 0) match {
          case false => Array.empty[DwellTime]
          case true => {

            val stateMachine = new DwellTimeStateMachine(pioneer.pioneer_id, entries.head)
            entries.tail.foreach { e =>
              try {stateMachine.addEvent(e)}
              catch {
                case ex: Exception => println(s"null pointer exception caused by ${pioneer.pioneer_id}, event: $e")
              }}

            stateMachine.end
          }
        }
      } catch {
        case ex: Exception => throw new Exception(s"Exception while Processing ${pioneer.pioneer_id}: $ex")
      }
    }
    dwell_times.write.mode("overwrite").parquet(s"${conf.outputPath()}")
  }
}