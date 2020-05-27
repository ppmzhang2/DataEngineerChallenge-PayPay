package com.paypay.quiz

import com.paypay.quiz.models.SessionizedLog
import javax.inject.Singleton
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


@Singleton
class Dao extends SparkService {
  import spark.implicits._

  lazy private val rawDs = getRawDs

  private def expand(interval: Long): UserDefinedFunction = udf {
    (start: Long, end: Long) => {
      start.to(end + interval).by(interval)
    }
  }

  def sessions(interval: Long): DataFrame = {
    rawDs.groupBy("client_ip", "user_agent")
      .agg(("timestamp", "min"), ("timestamp", "max"))
      .withColumnRenamed(existingName = "min(timestamp)",
        newName = "timestamp_start")
      .withColumnRenamed(existingName = "max(timestamp)",
        newName = "timestamp_end")
      .withColumn(colName = "client_id", monotonically_increasing_id())
      .withColumn(colName = "timestamp_start",
        col(colName = "timestamp_start").cast(to = "long"))
      .withColumn(colName = "timestamp_end",
        col(colName = "timestamp_end").cast(to = "long"))
      .withColumn(colName = "seq_timestamp",
        expand(interval = interval)(
          col(colName = "timestamp_start"),
          col(colName = "timestamp_end")))
      .select(
        col("client_id"),
        col("client_ip"),
        col("user_agent"),
        explode(col("seq_timestamp")))
      .withColumnRenamed(existingName = "col",
        newName = "session_start")
      .withColumn(colName = "session_end",
        col(colName = "session_start") + interval)
      .withColumn(colName = "session_id", monotonically_increasing_id())
  }

  def sessionizedDataset(interval: Long): Dataset[SessionizedLog] = {
    val log = rawDs.withColumn(colName = "timestamp",
      col(colName = "timestamp").cast(to = "Long")).as(alias = "left").cache()
    val sess = sessions(interval).as(alias = "right").cache()
    log.join(sess,
      col(colName = "left.client_ip") === col(colName = "right.client_ip") &&
        col(colName = "left.user_agent") === col(colName = "right.user_agent") &&
        col(colName = "left.timestamp") >= col(colName = "right.session_start") &&
        col(colName = "left.timestamp") <= col(colName = "right.session_end"))
      .select("right.session_id", "right.client_id", "left.timestamp",
        "left.elb", "left.client_ip", "left.client_port", "left.backend_ip",
        "left.backend_port", "left.request_processing_time",
        "left.backend_processing_time", "left.response_processing_time",
        "left.elb_status_code", "left.backend_status_code",
        "left.received_bytes", "left.sent_bytes", "left.request_action",
        "left.request_url", "left.request_protocol", "left.user_agent",
        "left.ssl_cipher", "left.ssl_protocol"
      ).as[SessionizedLog]
  }

  def aggBySession(interval: Long): DataFrame = {
    val sessionizedDs = sessionizedDataset(interval).cache
    sessionizedDs
      .groupBy("session_id", "client_id",
        "client_ip", "user_agent")
      .agg(("timestamp", "count"), ("timestamp", "min"),
        ("timestamp", "max"))
      .selectExpr("session_id", "client_id", "client_ip", "user_agent",
        "`min(timestamp)` AS session_start",
        "`max(timestamp)` AS session_end",
        "`count(timestamp)` AS counts")
      .withColumn(colName = "session_length",
        col(colName = "session_end").minus(col(colName = "session_start")))
  }

  def uniqueUrlHits(interval: Long): DataFrame = {
    val sessionizedDs = sessionizedDataset(interval).cache
    sessionizedDs.groupBy("session_id", "client_ip",
      "user_agent", "request_url")
      .agg(("timestamp", "min"))
      .groupBy("session_id", "client_ip", "user_agent")
      .agg(("request_url", "count"))
      .selectExpr("session_id", "client_ip", "user_agent",
        "`count(request_url)` AS unique_hits")
  }

  def mostEngagedUser(interval: Long): DataFrame = {
    aggBySession(interval)
      .groupBy("client_ip")
      .agg(("session_length", "sum"))
      .selectExpr("client_ip", "`sum(session_length)` AS length")
      .orderBy(col("length").desc)
      .limit(10)
  }

  def avgSessionLength(interval: Long): DataFrame = {
    aggBySession(interval)
      .select(avg(col(colName = "session_length")))
      .withColumnRenamed(existingName = "avg(session_length)",
        newName = "avg_length")
      .limit(1)
  }
}
