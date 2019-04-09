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
      if (!splitString.isEmpty && ccTlds.contains(splitString.last)) {
        splitString.takeRight(3).mkString(".")
      } else {
        splitString.takeRight(2).mkString(".")
      }
    }
  }

  val ccTlds = Seq(
    "ac", "ad", "ae", "af", "ag", "ai", "al", "am", "an", "ao", "aq", "ar", "as", "at", "au", "aw", "ax", "az", "ba",
    "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bl", "bm", "bn", "bo", "bq", "br", "bs", "bt", "bv", "bw", "by",
    "bz", "ca", "cc", "cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr", "cu", "cv", "cw", "cx", "cy",
    "cz", "de", "dj", "dk", "dm", "do", "dz", "ec", "ee", "eg", "eh", "er", "es", "et", "eu", "fi", "fj", "fk", "fm",
    "fo", "fr", "ga", "gb", "gd", "ge", "gf", "gg", "gh", "gi", "gl", "gm", "gn", "gp", "gq", "gr", "gs", "gt", "gu",
    "gw", "gy", "hk", "hm", "hn", "hr", "ht", "hu", "id", "ie", "il", "im", "in", "io", "iq", "ir", "is", "it", "je",
    "jm", "jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky", "kz", "la", "lb", "lc", "li", "lk",
    "lr", "ls", "lt", "lu", "lv", "ly", "ma", "mc", "md", "me", "mf", "mg", "mh", "mk", "ml", "mm", "mn", "mo", "mp",
    "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na", "nc", "ne", "nf", "ng", "ni", "nl", "no", "np",
    "nr", "nu", "nz", "om", "pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pn", "pr", "ps", "pt", "pw", "py", "qa",
    "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd", "se", "sg", "sh", "si", "sj", "sk", "sl", "sm", "sn", "so",
    "sr", "ss", "st", "su", "sv", "sx", "sy", "sz", "tc", "td", "tf", "tg", "th", "tj", "tk", "tl", "tm", "tn", "to",
    "tp", "tr", "tt", "tv", "tw", "tz", "ua", "ug", "uk", "um", "us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn",
    "vu", "wf", "ws", "한국", "ভারত", "বাংলা", "қаз", "срб", "бел", "சிங்கப்பூர்", "мкд", "中国", "中國", "భారత్", "ලංකා",
    "ભારત", "भारत", "укр", "香港", "台湾", "台灣", "мон", "الجزائر", "عمان", "ایران", "امارات", "پاکستان", "الاردن",
    "بھارت", "المغرب", "السعودية", "سودان", "عراق", "مليسيا", "გე", "ไทย", "سورية", "рф", "تونس", "ਭਾਰਤ", "مصر", "قطر",
    "இலங்கை", "இந்தியா", "հայ", "新加坡", "فلسطين", "ye", "yt", "za", "zm", "zw"
  )
}
