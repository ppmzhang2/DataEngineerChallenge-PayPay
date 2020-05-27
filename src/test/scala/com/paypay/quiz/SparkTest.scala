package com.paypay.quiz

import org.scalatest.funsuite.AnyFunSuite


class SparkTest extends AnyFunSuite {
  test("row count consistent") {
    val sparkService = new SparkService()
    val ds = sparkService.getRawDs

    assert(ds.count() === rowCount)
  }

  test("average session length") {
    val dao = new Dao()
    val res = dao.avgSessionLength(900).collect.head.getDouble(0)

    assert(res === avgSessionLen)
  }
}
