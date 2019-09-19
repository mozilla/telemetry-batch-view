/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry

import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

object SyncViewTestPayloads {
  // Test payloads, typically taken directly from about:telemetry.

  // An "old style" payload where each "ping" recorded exactly 1 sync.
  def singleSyncPing: JValue = parse("""{
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
  def multiSyncPing: JValue = parse(
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
    |    "os": {
    |      "name": "Darwin",
    |      "version": "15.6.0",
    |      "locale": "en-US"
    |    },
    |    "why": "schedule",
    |    "version": 1,
    |    "syncs": [
    |      {
    |        "when": 1473313854446,
    |        "uid": "12345678912345678912345678912345",
    |        "deviceID": "2222222222222222222222222222222222222222222222222222222222222222",
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
    |        "deviceID": "2222222222222222222222222222222222222222222222222222222222222222",
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

  // Sync ping payload with devices, steps, and validation data.
  def complexSyncPing: JValue = parse(
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
      |    "os": {
      |      "name": "Darwin",
      |      "version": "15.6.0",
      |      "locale": "en-US"
      |    },
      |    "why": "schedule",
      |    "version": 1,
      |    "syncs": [
      |      {
      |        "when": 1473313854446,
      |        "uid": "12345678912345678912345678912345",
      |        "took": 2277,
      |        "devices": [
      |          {
      |            "os": "iOS",
      |            "version": "0.1",
      |            "id": "1111111111111111111111111111111111111111111111111111111111111111"
      |          },
      |          {
      |            "os": "WINNT",
      |            "version": "52.0a1",
      |            "id": "2222222222222222222222222222222222222222222222222222222222222222"
      |          }
      |        ],
      |        "engines": [
      |          {
      |            "name": "clients",
      |            "took": 249
      |          },
      |          {
      |            "name": "passwords",
      |            "took": 50,
      |            "validation": {
      |              "version": 1,
      |              "checked": 10,
      |              "took": 30,
      |              "problems": []
      |            }
      |          },
      |          {
      |            "name": "tabs",
      |            "took": 16
      |          },
      |          {
      |            "name": "bookmarks",
      |            "took": 1000,
      |            "steps": [{
      |              "name": "fetchLocalTree",
      |              "took": 123,
      |              "counts": [{
      |                "name": "items",
      |                "count": 456
      |              }, {
      |                "name": "deletions",
      |                "count": 1
      |              }]
      |            }, {
      |              "name": "fetchRemoteTree",
      |              "took": 789
      |            }, {
      |              "name": "apply"
      |            }],
      |            "validation": {
      |              "version": 1,
      |              "checked": 290,
      |              "took": 723,
      |              "problems": [
      |                {
      |                  "name": "serverMissing",
      |                  "count": 20
      |                },
      |                {
      |                  "name": "rootOnServer",
      |                  "count": 1
      |                }
      |              ]
      |            }
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

  // This also stores the os at the top level for compatibility with android (bug 1409860)
  def multiSyncPingWithTopLevelIds: JValue = parse(
    """
      |{
      |  "type": "sync",
      |  "id": "a1d969b7-0084-4a78-a841-2abaf887f1b4",
      |  "creationDate": "2016-09-08T18:19:09.808Z",
      |  "version": 5,
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
      |  "os": {
      |    "name": "Darwin",
      |    "version": "15.6.0",
      |    "locale": "en-US"
      |  },
      |  "payload": {
      |    "why": "schedule",
      |    "version": 1,
      |    "uid": "12345678912345678912345678912345",
      |    "deviceID": "2222222222222222222222222222222222222222222222222222222222222222",
      |    "syncs": [
      |      {
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
