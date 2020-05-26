package com.paypay.quiz

import org.scalatest.funsuite.AnyFunSuite


class SparkTest extends AnyFunSuite {
  test("row count consistent") {
    val sparkService = new SparkService()
    val df = sparkService.getDF(BasePath)

    assert(df.count() === rowCount)
  }
}
