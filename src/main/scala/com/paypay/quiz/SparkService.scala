package com.paypay.quiz

import com.paypay.quiz.models.LogFmt
import javax.inject.Singleton
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}


@Singleton
class SparkService extends SparkSessionWrapper {

  import spark.implicits._

  private val logSchema = StructType(Array(
    StructField("timestamp", TimestampType, nullable = true),
    StructField("elb", StringType, nullable = true),
    StructField("client", StringType, nullable = true),
    StructField("backend", StringType, nullable = true),
    StructField("request_processing_time", FloatType, nullable = true),
    StructField("backend_processing_time", FloatType, nullable = true),
    StructField("response_processing_time", FloatType, nullable = true),
    StructField("elb_status_code", IntegerType, nullable = true),
    StructField("backend_status_code", IntegerType, nullable = true),
    StructField("received_bytes", IntegerType, nullable = true),
    StructField("sent_bytes", IntegerType, nullable = true),
    StructField("request", StringType, nullable = true),
    StructField("user_agent", StringType, nullable = true),
    StructField("ssl_cipher", StringType, nullable = true),
    StructField("ssl_protocol", StringType, nullable = true)))

  def getDS: Dataset[LogFmt] = {
    val df = getDF(BasePath)
      .withColumn(colName = "seq_client",
        split(col(colName = "client"), pattern = ":"))
      .withColumn(colName = "seq_backend",
        split(col(colName = "backend"), pattern = ":"))
      .withColumn(colName = "client_ip",
        col(colName = "seq_client").getItem(key = 0))
      .withColumn(colName = "client_port",
        col(colName = "seq_client").getItem(key = 1).cast(IntegerType))
      .withColumn(colName = "backend_ip",
        col(colName = "seq_backend").getItem(key = 0))
      .withColumn(colName = "backend_port",
        col(colName = "seq_backend").getItem(key = 1).cast(IntegerType))
      .withColumn(colName = "seq_request",
        split(col(colName = "request"), pattern = " "))
      .withColumn(colName = "request_action",
        col(colName = "seq_request").getItem(key = 0))
      .withColumn(colName = "request_url",
        col(colName = "seq_request").getItem(key = 1))
      .withColumn(colName = "request_protocol",
        col(colName = "seq_request").getItem(key = 2))
      .drop(colNames = "seq_client", "seq_backend", "client", "backend",
        "seq_request", "request")
    df.as[LogFmt]
  }

  private def getDF(path: String): DataFrame = {
    spark.read
      .schema(logSchema)
      .option("delimiter", " ")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = false)
      .csv(path)
  }
}
