package com.mozilla.telemetry.utils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

/**
 * A class that allows for easily reusable custom partitioning schemes.
 * 
 * Currently only works on DataFrames, future work will extend this
 * to Datasets.
 */
object CustomPartitioners {
  implicit class CustomRepartitioner(df: DataFrame) {
    def customRepartition(nPartitions: Int, partitionCol: String): DataFrame = {
      val rdd = df.rdd
        .map(r => (r, 1))
        .partitionBy(new org.apache.spark.Partitioner{
          override def numPartitions = nPartitions 
          override def getPartition(key: Any) = key.asInstanceOf[Row].getAs[Int](partitionCol)
        }).map(_._1)

      df.sparkSession.createDataFrame(rdd, df.schema)
    }

    def consistentRepartition(nPartitions: Int, partitionCol: String, hashFunc: String => Double): DataFrame = {
      val columnName = "consistentPartitionColumn"
      val udf = ConsistentPartitioner.getPartitioningFunc(nPartitions, hashFunc)

      df.withColumn(columnName, udf(col(partitionCol)))
        .customRepartition(nPartitions, columnName)
        .drop(columnName)
    }
  }
}
