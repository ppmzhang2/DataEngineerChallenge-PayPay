package com.paypay.quiz

object AnalyticalQuiz extends App {
  private val dao = new Dao()

  def saveSessionizedDs(): Unit = {
    val sessionziedLogPath = BasePath + "sessionized_log"
    dao.sessionizedDataset(interval = 900)
      .repartition(numPartitions = 1).write
      .format(source = "csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .save(sessionziedLogPath)
  }

  def saveUniqueHits(): Unit = {
    val uniqueHitsPath = BasePath + "unique_hits"
    dao.uniqueUrlHits(interval = 900)
      .repartition(numPartitions = 1).write
      .format(source = "csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .save(uniqueHitsPath)
  }

  def saveMostEngagedUser(): Unit = {
    val engagedUserPath = BasePath + "engaged_user"
    dao.mostEngagedUser(interval = 900)
      .repartition(numPartitions = 1).write
      .format(source = "csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .save(engagedUserPath)
  }

  def saveAvgSessionLength(): Unit = {
    val avgSessionLenPath = BasePath + "avg_session_length"
    dao.avgSessionLength(interval = 900)
      .repartition(numPartitions = 1).write
      .format(source = "csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", value = true)
      .save(avgSessionLenPath)
  }

  def main(): Unit = {
    saveSessionizedDs()
    saveUniqueHits()
    saveMostEngagedUser()
    saveAvgSessionLength()
  }

  main()
}
