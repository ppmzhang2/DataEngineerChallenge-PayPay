package com.paypay.quiz
import javax.inject.Singleton
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

@Singleton
class SparkService extends SparkSessionWrapper {

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

  def getDF(path: String): DataFrame = {
    spark.read
      .schema(logSchema)
      .option("delimiter", " ")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = false)
      .csv(path)
  }

}
