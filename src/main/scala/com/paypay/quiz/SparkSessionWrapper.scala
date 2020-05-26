package com.paypay.quiz

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("PayPay Data Engineer Challenge")
    .getOrCreate
}
