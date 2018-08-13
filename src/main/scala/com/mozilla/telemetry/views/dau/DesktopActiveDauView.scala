/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.views.dau

object DesktopActiveDauView extends GenericDauTrait {
  val jobName: String = "desktop_active_dau"

  def getGenericDauConf(args: Array[String]): GenericDauConf = {
    val conf = new BaseCliConf(args)
    conf.verify()
    GenericDauConf(
      conf.from.getOrElse(conf.to()),
      conf.to(),
      countColumn = Some("client_id"),
      inputBasePath = s"${conf.bucketProto()}${conf.bucket()}/clients_daily/v6",
      inputDateColumn = "submission_date_s3",
      outputBasePath = s"${conf.bucketProto()}${conf.bucket()}/desktop_active_dau/v1",
      where = "scalar_parent_browser_engagement_total_uri_count_sum >= 5"
    )
  }
}
