package com.paypay.quiz

import org.scalatest.funsuite.AnyFunSuite


class SparkTest extends AnyFunSuite {
  test("row count consistent") {
    val sparkService = new SparkService()
    val ds = sparkService.getDS

    assert(ds.count() === rowCount)
  }

  test("average session length") {
    val dao = new Dao()
    val res = dao.avg_session(900)

    assert(res === avgSessionLen)
  }
}
