/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

import org.rogach.scallop.{ScallopConf, ScallopOption}

object DesktopDauView extends GenericDauTrait {
  class CliConf(args: Array[String]) extends ScallopConf(args) {
    val to: ScallopOption[String] = opt[String](
      "to",
      descr = "newest date for with to generate counts (inclusive)",
      required = true)
    val from: ScallopOption[String] = opt[String](
      "from",
      descr = "oldest date for which to generate counts, default is `--to` value (inclusive)",
      required = false)
    val bucket: ScallopOption[String] = opt[String](
      "bucket",
      default = Some("s3://telemetry-parquet"),
      descr = "location where input and output data sets are stored",
      required = false)
    verify()
  }

  val jobName: String = "desktop_dau"

  def getGenericDauConf(args: Array[String]): GenericDauConf = {
    val conf = new CliConf(args)
    GenericDauConf(
      conf.from.getOrElse(conf.to()),
      conf.to(),
      inputBasePath = s"${conf.bucket()}/client_count_daily/v2",
      outputBasePath = s"${conf.bucket()}/desktop_dau/v1"
    )
  }
}
