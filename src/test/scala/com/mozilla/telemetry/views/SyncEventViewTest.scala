package com.mozilla.telemetry

import com.mozilla.telemetry.views.SyncEventConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.scalatest.{FlatSpec, Matchers}

class SyncEventViewTest extends FlatSpec with Matchers{
  def sync_payload = parse(
    """
      |{
      |  "type": "sync",
      |  "id": "a1d969b7-0084-4a78-a841-2abaf887f1b4",
      |  "creationDate": "2016-09-08T18:19:09.808Z",
      |  "version": 4,
      |  "application": {
      |    "architecture": "x86-64",
      |    "buildId": "20160907030427",
      |    "name": "Firefox",
      |    "version": "51.0a1",
      |    "displayVersion": "51.0a1",
      |    "vendor": "Mozilla",
      |    "platformVersion": "51.0a1",
      |    "xpcomAbi": "x86_64-msvc",
      |    "channel": "nightly"
      |  },
      |  "payload": {
      |    "why": "schedule",
      |    "version": 1,
      |    "uid": "12345678912345678912345678912345",
      |    "deviceID": "2222222222222222222222222222222222222222222222222222222222222222",
      |    "syncs": [
      |      {
      |        "devices": [
      |          {
      |            "os": "WINNT",
      |            "version": "55.0a1",
      |            "id": "3333333333333333333333333333333333333333333333333333333333333333"
      |          }
      |        ],
      |        "when": 1473313854446,
      |        "took": 2277,
      |        "engines": [
      |          {
      |            "name": "clients",
      |            "outgoing": [
      |              {
      |                "sent": 1
      |              }
      |            ],
      |            "took": 468
      |          },
      |          {
      |            "name": "passwords",
      |            "took": 16
      |          },
      |          {
      |            "name": "tabs",
      |            "incoming": {
      |              "applied": 2,
      |              "reconciled": 1
      |            },
      |            "outgoing": [
      |              {
      |                "sent": 1
      |              }
      |            ],
      |            "took": 795
      |          },
      |          {
      |            "name": "bookmarks"
      |          },
      |          {
      |            "name": "forms",
      |            "outgoing": [
      |              {
      |                "sent": 2
      |              }
      |            ],
      |            "took": 266
      |          },
      |          {
      |            "name": "history",
      |            "incoming": {
      |              "applied": 2
      |            },
      |            "outgoing": [
      |              {
      |                "sent": 2
      |              }
      |            ],
      |            "took": 514
      |          }
      |        ]
      |      },
      |      {
      |        "when": 1473313890947,
      |        "took": 484,
      |        "devices": [
      |          {
      |            "os": "iOS",
      |            "version": "7.1",
      |            "id": "4444444444444444444444444444444444444444444444444444444444444444"
      |          }
      |        ],
      |        "engines": [
      |          {
      |            "name": "clients",
      |            "took": 249
      |          },
      |          {
      |            "name": "passwords"
      |          },
      |          {
      |            "name": "tabs",
      |            "took": 16
      |          },
      |          {
      |            "name": "bookmarks"
      |          },
      |          {
      |            "name": "forms"
      |          },
      |          {
      |            "name": "history"
      |          }
      |        ]
      |      }
      |    ],
      |    "events": [
      |      [1234, "sync", "displayURI", "sendcommand", null,
      |        {
      |          "deviceID": "3333333333333333333333333333333333333333333333333333333333333333",
      |          "flowID": "aaaaaaaaaaaaa"
      |        }
      |      ],
      |      [2345, "sync", "displayURI", "processcommand", null,
      |       {
      |          "deviceID": "4444444444444444444444444444444444444444444444444444444444444444",
      |          "flowID": "bbbbbbbbbbbbbb"
      |        }
      |      ]
      |    ]
      |  }
      |}
    """.stripMargin)

  "Sync Events" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("SyncEventViewTest")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    implicit val formats = DefaultFormats
    sc.setLogLevel("WARN")
    try {
      val row = SyncEventConverter.pingToRows(sync_payload)
      val spark = SparkSession
        .builder()
        .appName("SyncEventsViewTest")
        .getOrCreate()
      val rdd = sc.parallelize(row)

      val dataframe = spark.createDataFrame(rdd, SyncEventConverter.syncEventSchema)

      // verify the contents.
      dataframe.count() should be (2)
      val checkRow = dataframe.first()

      checkRow.getAs[String]("document_id") should be ((sync_payload \ "id").extract[String])
      checkRow.getAs[String]("app_build_id") should be ((sync_payload \ "application" \ "buildId").extract[String])
      checkRow.getAs[String]("app_display_version") should be ((sync_payload \ "application" \ "displayVersion").extract[String])
      checkRow.getAs[String]("app_name") should be ((sync_payload \ "application" \ "name").extract[String])
      checkRow.getAs[String]("app_version") should be ((sync_payload \ "application" \ "version").extract[String])
      checkRow.getAs[String]("app_channel") should be ((sync_payload \ "application" \ "channel").extract[String])

      val payload = sync_payload \ "payload"
      checkRow.getAs[String]("why") should be ((payload \ "why").extract[String])
      checkRow.getAs[String]("uid") should be ((payload \ "uid").extract[String])
      checkRow.getAs[String]("device_id") should be ((payload \ "deviceID").extract[String])

      val event = (payload \ "events")(0)
      checkRow.getAs[Long]("event_timestamp") should be (event(0).extract[Long])
      checkRow.getAs[String]("event_category") should be (event(1).extract[String])
      checkRow.getAs[String]("event_method") should be (event(2).extract[String])
      checkRow.getAs[String]("event_object") should be (event(3).extract[String])
      checkRow.getAs[String]("event_string_value") should be (null)
      checkRow.getAs[Map[String, String]]("event_map_values") should be (event(5).extract[Map[String, String]])
      checkRow.getAs[String]("event_device_id") should be ((event(5) \ "deviceID").extract[String])
      checkRow.getAs[String]("event_flow_id") should be ((event(5) \ "flowID").extract[String])
      checkRow.getAs[String]("event_device_version") should be ("55.0a1")
      checkRow.getAs[String]("event_device_os") should be ("WINNT")
    } finally {
      sc.stop()
    }
  }
}
