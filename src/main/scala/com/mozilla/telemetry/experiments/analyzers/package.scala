package com.mozilla.telemetry.experiments

import scala.collection.Map

package object analyzers {
  def addHistograms[T](l: Map[T, Long], r: Map[T, Long]): Map[T, Long] = {
    l ++ r.map { case (k, v) => k -> (v + l.getOrElse(k, 0L)) }
  }
}
