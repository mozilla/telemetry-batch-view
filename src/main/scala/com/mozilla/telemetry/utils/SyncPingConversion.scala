package com.mozilla.telemetry.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST.{JArray, JInt, JObject, JString}
import java.util.UUID

// Common conversion code for SyncView and SyncFlatView. Schema for sync pings described here:
// https://dxr.mozilla.org/mozilla-central/source/services/sync/tests/unit/sync_ping_schema.json
object SyncPingConversion {
  /*
   * The type definitions for the rows we create.
   */
  private val failureType = StructType(List(
    // failures are probably *too* flexible in the schema, but all current errors have a "name" and a second field
    // that is a string or an int. To keep things simple and small here, we just define a string "value" field and
    // convert ints to the string.
    StructField("name", StringType, nullable = false),
    StructField("value", StringType, nullable = true)
  ))

  // The record of incoming sync-records.
  private val incomingType = StructType(List(
    StructField("applied", LongType, nullable = false),
    StructField("failed", LongType, nullable = false),
    StructField("new_failed", LongType, nullable = false),
    StructField("reconciled", LongType, nullable = false)
  ))

  // Outgoing records.
  private val outgoingType = StructType(List(
    StructField("sent", LongType, nullable = false),
    StructField("failed", LongType, nullable = false)
  ))

  // Entries in devices array
  private val deviceType = StructType(List(
    StructField("id", StringType, nullable = false),
    StructField("version", StringType, nullable = false),
    StructField("os", StringType, nullable = false)
  ))

  // Data about a single validation problem found
  private val validationProblemType = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("count", LongType, nullable = false)
  ))

  // Data about a validation run on an engine
  private val validationType = StructType(List(
    // Validator version, optional per spec, but we fill in 0 where it was missing.
    StructField("version", LongType, nullable = false),
    StructField("checked", LongType, nullable = false), // # records checked
    StructField("took", LongType, nullable = false), // milliseconds
    StructField("problems", ArrayType(validationProblemType, containsNull = false), nullable = true),
    // present if the validator failed for some reason.
    StructField("failure_reason", failureType, nullable = true)
  ))

  // The schema for an engine.
  private val engineType = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("took", LongType, nullable = false),
    StructField("status", StringType, nullable = true),
    StructField("failure_reason", failureType, nullable = true),
    StructField("incoming", incomingType, nullable = true),
    StructField("outgoing", ArrayType(outgoingType, containsNull = false), nullable = true),
    StructField("validation", validationType, nullable = true)
  ))

  // The status for the Sync itself (ie, not the status for an engine - that's just a string)
  private val statusType = StructType(List(
    StructField("sync", StringType, nullable = true),
    StructField("service", StringType, nullable = true)
  ))

  // The record of a single sync event.
  def nestedSyncType = StructType(List(
    // These field names are the same as used by MainSummaryView
    StructField("app_build_id", StringType, nullable = true), // application/buildId
    StructField("app_display_version", StringType, nullable = true), // application/displayVersion
    StructField("app_name", StringType, nullable = true), // application/name
    StructField("app_version", StringType, nullable = true), // application/version
    StructField("app_channel", StringType, nullable = true), // application/channel

    StructField("os", StringType, nullable = true), // os/name
    StructField("os_version", StringType, nullable = true), // os/version
    StructField("os_locale", StringType, nullable = true), // os/locale

    // These fields are unique to the sync pings.
    StructField("uid", StringType, nullable = false),
    StructField("device_id", StringType, nullable = true), // should always exists, but old pings didn't record it.
    StructField("when", LongType, nullable = false),
    StructField("took", LongType, nullable = false),
    StructField("failure_reason", failureType, nullable = true),
    StructField("status", statusType, nullable = true),
    // "why" is defined in the client-side schema but currently never populated.
    StructField("why", StringType, nullable = true),
    StructField("engines", ArrayType(engineType, containsNull = false), nullable = true),
    StructField("devices", ArrayType(deviceType, containsNull = false), nullable = true)
  ))

  def singleEngineFlatSyncType = StructType(List(
    // These field names are the same as used by MainSummaryView
    StructField("app_build_id", StringType, nullable = true), // application/buildId
    StructField("app_display_version", StringType, nullable = true), // application/displayVersion
    StructField("app_name", StringType, nullable = true), // application/name
    StructField("app_version", StringType, nullable = true), // application/version
    StructField("app_channel", StringType, nullable = true), // application/channel

    StructField("os", StringType, nullable = true), // os/name
    StructField("os_version", StringType, nullable = true), // os/version
    StructField("os_locale", StringType, nullable = true), // os/locale

    // These fields are unique to the sync pings.
    StructField("uid", StringType, nullable = false),
    StructField("device_id", StringType, nullable = true), // should always exists, but old pings didn't record it.
    StructField("when", LongType, nullable = false),
    StructField("took", LongType, nullable = false),
    StructField("failure_reason", failureType, nullable = true),
    StructField("status", statusType, nullable = true),
    // "why" is defined in the client-side schema but currently never populated.
    StructField("why", StringType, nullable = true),
    StructField("devices", ArrayType(deviceType, containsNull = false), nullable = true),

    StructField("sync_id", StringType, nullable = false),
    StructField("sync_day", StringType, nullable = false), // `when` formatted as "yyyyMMdd".

    StructField("engine_name", StringType, nullable = false),
    StructField("engine_took", LongType, nullable = false),
    StructField("engine_status", StringType, nullable = true),
    StructField("engine_failure_reason", failureType, nullable = true),

    StructField("engine_incoming_applied", LongType, nullable = false),
    StructField("engine_incoming_failed", LongType, nullable = false),
    StructField("engine_incoming_new_failed", LongType, nullable = false),
    StructField("engine_incoming_reconciled", LongType, nullable = false),

    StructField("engine_outgoing_batch_count", LongType, nullable = false),
    StructField("engine_outgoing_batch_total_sent", LongType, nullable = false),
    StructField("engine_outgoing_batch_total_failed", LongType, nullable = false)
  ))

 /*
 * Convert the JSON payload to rows matching the above types.
 */
  // XXX - this looks dodgy - I'm sure there's a more scala-ish way to write this...
  private def failureReasonToRow(failure: JValue): Row = failure match {
    case JObject(x) =>
      implicit val formats: DefaultFormats.type = DefaultFormats
      Row(
        (failure \ "name").extract[String],
        (failure \ "name").extract[String] match {
          case "httperror" => (failure \ "code").extract[String]
          case "nserror" => (failure \ "code").extract[String]
          case "shutdownerror" => null
          case "autherror" => (failure \ "from").extract[String]
          case "othererror" => (failure \ "error").extract[String]
          case "unexpectederror" => (failure \ "error").extract[String]
          case "sqlerror" => (failure \ "code").extract[String]
          case _ => null
        }
      )
    case _ =>
      null
  }

  private def deviceToRow(device: JValue): Option[Row] = device match {
    case JObject(d) =>
      Some(Row(
        device \ "id" match {
          case JString(x) => x
          case _ => return None
        },
        device \ "version" match {
          case JString(x) => x
          case _ => return None
        },
        device \ "os" match {
          case JString(x) => x
          case _ => return None
        }
      ))
    case _ => None
  }

  private def toDeviceRows(devices: JValue): List[Row] = devices match {
    case JArray(x) =>
      val rows = x.flatMap(d => deviceToRow(d))
      if (rows.isEmpty) null
      else rows
    case _ => null
  }

  private def incomingSummary(incoming: JValue): (Long, Long, Long, Long) = incoming match {
    case JObject(_) =>
      (
        incoming \ "applied" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "failed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "newFailed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "reconciled" match {
          case JInt(x) => x.toLong
          case _ => 0L
        }
      )
    case _ => (0L, 0L, 0L, 0L)
  }

  // Create a row representing incomingType
  private def incomingToRow(incoming: JValue): Row = incoming match {
    case JObject(_) =>
      val (applied, failed, newFailed, reconciled) = incomingSummary(incoming)
      Row(applied, failed, newFailed, reconciled)
    case _ => null
  }

  private def parseSingleOutgoing(outgoingEntry: JValue): (Long, Long) = {
    val sent = outgoingEntry \ "sent" match {
      case JInt(n) => n.toLong
      case _ => 0L
    }
    val failed = outgoingEntry \ "failed" match {
      case JInt(n) => n.toLong
      case _ => 0L
    }
    (sent, failed)
  }

  private def outgoingSummary(outgoing: JValue): (Long, Long, Long) = outgoing match {
    case JArray(x) =>
      val (totalSent, totalFailed) = x.foldLeft((0L, 0L))((acc, entry) => {
        val (entrySent, entryFailed) = parseSingleOutgoing(entry)
        (acc._1 + entrySent, acc._2 + entryFailed)
      })
      (x.size.toLong, totalSent, totalFailed)

    case JObject(_) =>
      val (entrySent, entryFailed) = parseSingleOutgoing(outgoing)
      (1L, entrySent, entryFailed)

    case _ =>
      (0L, 0L, 0L)
  }

  private def outgoingToRow(outgoing: JValue): List[Row] = outgoing match {
    case JArray(x) if x.nonEmpty =>
      x.map(o => {
        val (sent, failed) = parseSingleOutgoing(o)
        Row(sent, failed)
      })
    case JObject(_) =>
      val (sent, failed) = parseSingleOutgoing(outgoing)
      List(Row(sent, failed))
    case _ =>
      null
  }

  private def validationToRow(validation: JValue): Row = validation match {
    case JObject(_) =>
      Row(
        validation \ "version" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        validation \ "checked" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        validation \ "took" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        validation \ "problems" match {
          case JArray(problems) =>
            problems.flatMap(validationProblemToRow)
          case _ => null
        },
        failureReasonToRow(validation \ "failureReason")
      )
    case _ => null
  }

  private def validationProblemToRow(problem: JValue): Option[Row] = problem match {
    case JObject(_) =>
      Some(Row(
        problem \ "name" match {
          case JString(x) => x
          case _ => return None
        },
        problem \ "count" match {
          case JInt(x) => x.toLong
          case _ => return None
        }
      ))
    case _ => None
  }

  // Parse an element of "engines" elt in a sync object
  private def engineToRow(engine: JValue): Row = {
    Row(
      engine \ "name" match {
        case JString(x) => x
        case _ => return null // engines must have a name!
      },
      engine \ "took" match {
        case JInt(x) => x.toLong
        case _ => 0L
      },
      engine \ "status" match {
        case JString(x) => x
        case _ => null
      },
      failureReasonToRow(engine \ "failureReason"),
      incomingToRow(engine \ "incoming"),
      outgoingToRow(engine \ "outgoing"),
      validationToRow(engine \ "validation")
    )
  }

  private def toEnginesRows(engines: JValue): List[Row] = engines match {
    case JArray(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      // Need simple array iteration??
      for (e <- x) {
        buf.append(engineToRow(e))
      }
      if (buf.isEmpty) null
      else buf.toList
    case _ => null
  }

  private def statusToRow(status: JValue): Row = status match {
    case JObject(_) =>
      Row(
        status \ "sync" match {
          case JString(x) => x
          case _ => null
        },
        status \ "service" match {
          case JString(x) => x
          case _ => null
        }
      )
    case _ => null
  }

  // Convert a "new style v1" ping that records multiple Syncs to a number of (nested) rows.
  private def multiSyncPayloadToNestedRow(ping: JValue, syncs: List[JValue]): List[Row] = {
    syncs.flatMap(sync => singleSyncPayloadToNestedRow(ping, sync))
  }

  // Convert an "old style" ping that records a single Sync to a (nested) row
  private def singleSyncPayloadToNestedRow(ping: JValue, sync: JValue): Option[Row] = {
    val application = ping \ "application"
    val payload = ping \ "payload"

    def stringFromSyncOrPayload(s: String): String = {
      sync \ s match {
        case JString(x) => x
        case _ =>
          payload \ s match {
            case JString(x) => x
            case _ => null
          }
      }
    }

    val row = Row(
      // The metadata...
      application \ "buildId" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "displayVersion" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "name" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "version" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      application \ "channel" match {
        case JString(x) => x
        case _ => return None // a required field.
      },

      payload \ "os" \ "name" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "os" \ "version" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "os" \ "locale" match {
        case JString(x) => x
        case _ => null
      },

      // Info about the sync.
      stringFromSyncOrPayload("uid") match {
        case null => return None // a required field.
        case x => x
      },

      stringFromSyncOrPayload("deviceID"),

      sync \ "when" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      sync \ "took" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      failureReasonToRow(sync \ "failureReason"),
      statusToRow(sync \ "status"),
      sync \ "why" match {
        case JString(x) => x
        case _ => null
      },
      toEnginesRows(sync \ "engines"),
      sync \ "devices" match {
        case devices @ JArray(_) => toDeviceRows(devices)
        case _ => null
      }
    )

    Some(row)
  }

  // Same as singleSyncPayloadToNestedRow, but creates a flat row.
  private def singleSyncPayloadToFlatRows(ping: JValue, sync: JValue): List[Row] = {
    val application = ping \ "application"
    val payload = ping \ "payload"

    def stringFromSyncOrPayload(s: String): String = {
      sync \ s match {
        case JString(x) => x
        case _ =>
          payload \ s match {
            case JString(x) => x
            case _ => null
          }
      }
    }
    val when = sync \ "when" match {
      case JInt(x) => x.toLong
      case _ => return List.empty
    }

    val syncDay = new DateTime(when).toDateTime.toString("yyyyMMdd")

    val rowRepeatedPart = List(
      // The metadata...
      application \ "buildId" match {
        case JString(x) => x
        case _ => return List.empty // a required field.
      },
      application \ "displayVersion" match {
        case JString(x) => x
        case _ => return List.empty // a required field.
      },
      application \ "name" match {
        case JString(x) => x
        case _ => return List.empty // a required field.
      },
      application \ "version" match {
        case JString(x) => x
        case _ => return List.empty // a required field.
      },
      application \ "channel" match {
        case JString(x) => x
        case _ => return List.empty // a required field.
      },

      payload \ "os" \ "name" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "os" \ "version" match {
        case JString(x) => x
        case _ => null
      },
      payload \ "os" \ "locale" match {
        case JString(x) => x
        case _ => null
      },

      // Info about the sync.
      stringFromSyncOrPayload("uid") match {
        case null => return List.empty // a required field.
        case x => x
      },

      stringFromSyncOrPayload("deviceID"),
      when,
      sync \ "took" match {
        case JInt(x) => x.toLong
        case _ => return List.empty
      },
      failureReasonToRow(sync \ "failureReason"),
      statusToRow(sync \ "status"),
      sync \ "why" match {
        case JString(x) => x
        case _ => null
      },
      sync \ "devices" match {
        case devices @ JArray(_) => toDeviceRows(devices)
        case _ => null
      },
      sync \ "sync_id" match {
        case JString(s) => s
        case _ => UUID.randomUUID.toString
      },
      syncDay
    )

    val engineParts = sync \ "engines" match {
      case JArray(a) =>
        a.map(engine => {
          val (batchesOut, sentOut, failedOut) = outgoingSummary(engine \ "outgoing")
          val (appliedIn, failedIn, newFailedIn, reconciledIn) = incomingSummary(engine \ "incoming")
          List(
            engine \ "name" match {
              case JString(x) => x
              case _ => return null // engines must have a name!
            },
            engine \ "took" match {
              case JInt(x) => x.toLong
              case _ => 0L
            },
            engine \ "status" match {
              case JString(x) => x
              case _ => null
            },
            failureReasonToRow(engine \ "failureReason"),

            appliedIn, failedIn, newFailedIn, reconciledIn,
            batchesOut,
            sentOut,
            failedOut
          )
        })
      case _ => return List.empty
    }

    engineParts.map(engineData =>
      Row.fromSeq(rowRepeatedPart ++ engineData))
  }

  // Same as multiSyncPayloadToNestedRow, but creates flat rows.
  private def multiSyncPayloadToFlatRows(ping: JValue, syncs: List[JValue]): List[Row] = {
    syncs.flatMap(sync => singleSyncPayloadToFlatRows(ping, sync))
  }

  // Take an entire ping and return a list of rows with "nestedSyncType" as a schema.
  def pingToNestedRows(ping: JValue): List[Row] = {
    ping \ "payload" \ "syncs" match {
      case JArray(x) => multiSyncPayloadToNestedRow(ping, x)
      case _ =>
        val row = singleSyncPayloadToNestedRow(ping, ping \ "payload")
        row match {
          case Some(x) => List(x)
          case None => List()
        }
    }
  }

  // Take an entire ping and return a list of rows with "singleEngineFlatSyncType" as a schema.
  def pingToFlatRows(ping: JValue): List[Row] = {
    ping \ "payload" \ "syncs" match {
      case JArray(x) => multiSyncPayloadToFlatRows(ping, x)
      case _ => singleSyncPayloadToFlatRows(ping, ping \ "payload")
    }
  }

}
