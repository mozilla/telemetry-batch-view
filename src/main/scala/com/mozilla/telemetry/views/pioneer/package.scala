/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.mozilla.telemetry.views

import scala.util.{Success, Try}

package object pioneer {
  case class EntryKey(pioneer_id: String, entry_timestamp: Long, branch: String, details: String, url: String)
  case class ExplodedEntry(ping_timestamp: Long, document_id: String, pioneer_id: String, study_name: String,
                           geo_city: String, geo_country: String, submission_date_s3: String, entry_timestamp: Long,
                           branch: String, details: String, url: String) {
    def toKey: EntryKey = EntryKey(pioneer_id, entry_timestamp, branch, details, url)
    def toLogEvent: LogEvent = LogEvent(entry_timestamp, branch, details, url, document_id)
  }

  case class LogEvent(timestamp: Long, branch: String, details: String, url: String, document_id: String) {
    def getDomain: String = {
      // We're wrapping this in an Option AND Try because java.net.URI can return a null and throw an exception
      val hostString = Try(Option(new java.net.URI(url).getHost)) match {
        case Success(Some(u)) => u
        // We've seen some urls with unencoded characters cause exceptions -- try just grabbing front of url
        case _ => Try(Option(new java.net.URI(url.split('/').take(3).mkString("/")).getHost)) match {
          case Success(Some(u)) => u
          case _ => url.split('/')(2)
        }
      }
      val splitString = hostString.split('.') // if you use a string instead of char it'll be interpreted as a regex
      splitString.takeRight(2).mkString(".")
    }
  }
}
