package com.mozilla.telemetry.views.pioneer

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopConf


object OnlineNewsV2DwellTimeView {
  val jobName = "online_news_dwell_time_view"
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass.getSimpleName)

  case class DwellTime(pioneer_id: String, branch: String, document_ids: List[String], visit_start_date: java.sql.Date, domain: String,
                       visit_start_time: Long, total_dwell_time: Long, total_idle_time: Long, nav_event_count: Int,
                       days_since_appearance: Int, log_events: List[LogEvent])

  class BranchSwitchException(e: String) extends Exception(e)
  class UnexpectedStateException(e: String) extends Exception(e)

  class Visit(pioneerId: String, initial_event: LogEvent, firstAppearance: Long) extends java.io.Serializable {
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
      val (navEvents, _) = events.foldLeft((0, "")) {
        case ((navs: Int, prev: String), e: LogEvent) => (navs + (if (prev == e.url) 0 else 1), e.url)
      }

      // Check to make sure there's no branch switching
      val branches = events.map(_.branch)
      if (!branches.forall(_ == branches.head)) {
        throw new BranchSwitchException("Inconsistent Log Entry Branches")
      }

      DwellTime(pioneerId, branches.head, documentIds.toList, visitStartDate, domain, visitStartTime, totalDwellTime,
        totalIdleTime, navEvents, ((visitStartTime - firstAppearance)/86400).toInt, events.toList)
    }
  }

  class DwellTimeStateMachine(pioneer_id: String, firstEvent: LogEvent) extends java.io.Serializable {

    sealed trait DwellTimeState

    case object VisitActive extends DwellTimeState

    case object VisitIdle extends DwellTimeState

    case object NotInVisit extends DwellTimeState

    sealed trait UrlState

    case object SameDomain extends UrlState

    case object DifferentDomain extends UrlState

    val idleThreshold = 30 * 60 // 30 minutes idle = new visit

    var currentVisit: Option[Visit] = Some(new Visit(pioneer_id, firstEvent, firstEvent.timestamp))
    var lastEvent: LogEvent = firstEvent
    var currentState: DwellTimeState = VisitActive
    var dwellTimes: Array[DwellTime] = Array()
    var allDocumentIds: Set[String] = Set(firstEvent.document_id)

    def addEvent(event: LogEvent): Unit = {
      val event_details = event.details
      val url_state = compareUrls(lastEvent, event)
      val delta = event.timestamp - lastEvent.timestamp

      if (delta > idleThreshold) {
        (event_details, url_state) match {
          /** * Handle events > 30Mins since the last event ***/
          case ("focus-start", _) if delta > idleThreshold =>
            endCurrentVisitAndActivateNew(event)
          case ("idle-start", _) if delta > idleThreshold =>
            endCurrentVisit
            newIdleVisit(event)
          case ("idle-end", _) if delta > idleThreshold =>
            endCurrentVisit
            newIdleVisit(event)
          case ("focus-end", _) if delta > idleThreshold =>
            endCurrentVisit
        }
      } else {
        currentState match {
          case VisitActive =>
            (event_details, url_state) match {
              /** * focus-start events ***/
              // new nav event
              case ("focus-start", SameDomain) =>
                currentVisit.map(_.addActiveTime(event))
              // new nav event
              case ("focus-start", DifferentDomain) =>
                endCurrentVisitAndActivateNew(event)

              /** * idle-start events ***/
              case ("idle-start", SameDomain) =>
                currentVisit.map(_.addActiveTime(event))
                currentState = VisitIdle
              case ("idle-start", DifferentDomain) =>
                endCurrentVisit
                newIdleVisit(event)

              /** * idle-end events ***/
              // this one is odd, and not something we expect to see but it happens a lot
              case ("idle-end", SameDomain) =>
                currentVisit.map(_.addIdleTime(event))
                currentState = VisitIdle
              case ("idle-end", DifferentDomain) =>
                endCurrentVisitAndActivateNew(event)

              /** * focus-end events ***/
              case ("focus-end", SameDomain) =>
                currentVisit.map(_.addActiveTime(event))
                endCurrentVisit
              case ("focus-end", DifferentDomain) =>
                endCurrentVisit

              case _ =>
                throw new UnexpectedStateException(
                  s"Unexpected state addition: $event to current state $currentState. url_state: $url_state, delta: $delta")
            }
          case VisitIdle =>
            (event_details, url_state) match {

              /** * focus-start events ***/
              case ("focus-start", SameDomain) =>
                activateIdleVisit(event)
              case ("focus-start", DifferentDomain) =>
                currentVisit.map(_.addIdleTime(event))
                endCurrentVisitAndActivateNew(event)

              /** * idle-start events ***/
              case ("idle-start", SameDomain) =>
                currentVisit.map(_.addIdleTime(event))
              case ("idle-start", DifferentDomain) =>
                currentVisit.map(_.addIdleTime(event))
                endCurrentVisit
                newIdleVisit(event)

              /** * idle-end events ***/
              case ("idle-end", SameDomain) =>
                currentVisit.map(_.addIdleTime(event))
              case ("idle-end", DifferentDomain) =>
                currentVisit.map(_.addIdleTime(event))
                endCurrentVisitAndActivateNew(event)

              /** * focus-end events ***/
              case ("focus-end", SameDomain) =>
                currentVisit.map(_.addIdleTime(event))
                endCurrentVisit
              case ("focus-end", DifferentDomain) =>
                endCurrentVisit

              case _ =>
                throw new UnexpectedStateException(
                  s"Unexpected state addition: $event to current state $currentState. url_state: $url_state, delta: $delta")
            }
          case NotInVisit =>
            (event_details, url_state) match {
              case ("focus-start", _) =>
                newVisit(event)
              case ("idle-start", _) =>
                newIdleVisit(event)
              case ("idle-end", _) =>
                newVisit(event)
              case _ =>
                ()
            }
        }
      }

      allDocumentIds += event.document_id
      lastEvent = event
    }

    def compareUrls(oldEvent: LogEvent, newEvent: LogEvent): UrlState = {
      if (oldEvent.getDomain == newEvent.getDomain) SameDomain
      else DifferentDomain
    }

    def endCurrentVisitAndActivateNew(event: LogEvent) = {
      dwellTimes ++= currentVisit.map(_.asDwellTime)
      newVisit(event)
    }

    def endCurrentVisit = {
      dwellTimes ++= currentVisit.map(_.asDwellTime)
      currentVisit = None
      currentState = NotInVisit
    }

    def activateIdleVisit(event: LogEvent, addNav: Boolean = false) = {
      currentVisit.map(_.addIdleTime(event))
      currentState = VisitActive
    }

    def newVisit(event: LogEvent) = {
      currentVisit = Some(new Visit(pioneer_id, event, firstEvent.timestamp))
      currentState = VisitActive
    }

    def newIdleVisit(event: LogEvent) = {
      currentVisit = Some(new Visit(pioneer_id, event, firstEvent.timestamp))
      currentState = VisitIdle
    }

    // Maps the Option[Visit], then folds left to add it to dwellTimes if it's non-empty
    def end: Array[DwellTime] = {
      dwellTimes ++= currentVisit.map(_.asDwellTime)
      dwellTimes
    }
  }

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val inputBucket = opt[String]("inbucket", descr = "Source bucket for pioneer online news data", required = true)
    val outputBucket = opt[String]("outbucket", descr = "Output path for parquet data", required = true)
    val submissionDate = opt[String]("date", descr = "Submission Date to Process", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    // Note these are tuned for c3.4xlarge and the memory characteristics of this job
    sparkConf.set("spark.executor.memory", "20G")
    sparkConf.set("spark.memory.storageFraction", "0.6")
    sparkConf.set("spark.memory.fraction", "0.6")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.sql.shuffle.partitions", "640")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val conf = new Conf(args)
    import spark.implicits._
    val actions = List("focus-start", "focus-end", "idle-start", "idle-end")

    val inputPath = {
      val path = s"s3://${conf.inputBucket()}/online_news_v2/deduped_daily/"
      conf.submissionDate.get match {
        case Some(date) => path + s"submission_date_s3=$date/"
        case _ => path
      }
    }

    val online_news_entries = spark.read.parquet(inputPath).as[ExplodedEntry]

    val flattenedEvents = online_news_entries
      .groupByKey(_.pioneer_id)

    val dwell_times: Dataset[DwellTime] = online_news_entries.groupByKey(_.pioneer_id).flatMapGroups {
      (pioneer_id, entries) => try {
        val sorted = entries
          .filter(e => actions.contains(e.details))
          .map(_.toLogEvent)
          .toIndexedSeq
          .sortBy(_.timestamp)

        (sorted.length > 0) match {
          case false => None
          case true => {
            val stateMachine = new DwellTimeStateMachine(pioneer_id, sorted.head)
            sorted.tail.foreach { e => stateMachine.addEvent(e) }
            val visits = stateMachine.end
            if (visits.tail.forall(_.branch != visits.head.branch))
              throw new BranchSwitchException("Inconsistent Branches between visits")
            else
              visits
          }
        }
      } catch {
        case _: BranchSwitchException => None
        case ex: Exception => throw new Exception(s"Exception while Processing ${pioneer_id}: $ex")
      }
    }
    val outputPath = conf.submissionDate.get match {
      case Some(date) => s"online_news_v2/dwell_time_daily/submission_date_s3=$date"
      case _ => s"online_news_v2/dwell_time_complete/"
    }
    dwell_times.write.mode("overwrite").parquet(outputPath)
  }
}