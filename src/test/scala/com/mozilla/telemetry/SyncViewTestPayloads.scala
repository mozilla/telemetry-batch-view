package com.mozilla.telemetry

import org.json4s.jackson.JsonMethods._

object SyncViewTestPayloads {
  // Test payloads, typically taken directly from about:telemetry.

  // An "old style" payload where each "ping" recorded exactly 1 sync.
  def singleSyncPing = parse("""{
    |  "type": "sync",
    |  "id": "824bb059-d22f-4930-b915-45580ecf463e",
    |  "creationDate": "2016-09-02T04:35:18.787Z",
    |  "version": 4,
    |  "application": {
    |    "architecture": "x86-64",
    |    "buildId": "20160831030224",
    |    "name": "Firefox",
    |    "version": "51.0a1",
    |    "displayVersion": "51.0a1",
    |    "vendor": "Mozilla",
    |    "platformVersion": "51.0a1",
    |    "xpcomAbi": "x86_64-msvc",
    |    "channel": "nightly"
    |  },
    |  "payload": {
    |    "when": 1472790916859,
    |    "uid": "123456789012345678901234567890",
    |    "took": 1918,
    |    "version": 1,
    |    "status": {
    |        "service": "error.sync.failed_partial",
    |        "sync": "error.login.reason.network"
    |    },
    |    "engines": [
    |      {
    |        "name": "clients",
    |        "took": 203
    |      },
    |      {
    |        "name": "passwords",
    |        "took": 123
    |      },
    |      {
    |        "name": "tabs",
    |        "incoming": {
    |          "applied": 2,
    |          "failed": 1
    |        },
    |        "outgoing": [
    |          {
    |            "sent": 1
    |          }
    |        ],
    |        "took": 624
    |      },
    |      {
    |        "name": "bookmarks",
    |        "took": 15,
    |        "failureReason": {
    |            "name": "nserror",
    |            "code": 2152398878
    |        },
    |        "status": "error.engine.reason.unknown_fail"
    |      },
    |      {
    |        "name": "forms",
    |        "outgoing": [
    |          {
    |            "sent": 1
    |          }
    |        ],
    |        "took": 250
    |      },
    |      {
    |        "name": "history",
    |        "outgoing": [
    |          {
    |            "sent": 6
    |          }
    |        ],
    |        "took": 249
    |      }
    |    ]
    |  }
    |}""".stripMargin)

  // A "new style" payload where there may be any number of syncs in a single ping.
  def multiSyncPing = parse(
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
    |    "syncs": [
    |      {
    |        "when": 1473313854446,
    |        "uid": "12345678912345678912345678912345",
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
    |        "uid": "12345678912345678912345678912345",
    |        "took": 484,
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
    |    ]
    |  }
    |}
    """.stripMargin)

}
