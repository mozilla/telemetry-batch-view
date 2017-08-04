package com.mozilla.telemetry.experiments.analyzers

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, lit, count, countDistinct}


case class ExperimentMetadata(experiment_id: String,
                              branch: String,
                              subgroup: String,
                              client_count: Long,
                              ping_count: Long)

object ExperimentAnalyzer {
  def getExperimentMetadata(df: DataFrame): Dataset[MetricAnalysis] = {
    import df.sparkSession.implicits._

    val counts = df
      .select(
        col("experiment_id"),
        col("experiment_branch").as("branch"),
        lit("All").as("subgroup"),
        col("client_id"))
      .groupBy(col("experiment_id"), col("branch"), col("subgroup"))
      .agg(countDistinct("client_id").alias("client_count"), count("*").alias("ping_count"))
      .as[ExperimentMetadata]

    counts.map { r =>
      MetricAnalysis(r.experiment_id, r.branch, r.subgroup, r.ping_count, "Experiment Metadata", "Metadata",
        Map.empty[Long, HistogramPoint],
        Some(Seq(
          Statistic(None, "Total Pings", r.ping_count.toDouble),
          Statistic(None, "Total Clients", r.client_count.toDouble)
        ))
      )
    }
  }
}
