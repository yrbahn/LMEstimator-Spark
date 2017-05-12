package com.kakao.sparklm.rdd

import com.kakao.sparklm.util.SparkLMConfig
import org.apache.spark.rdd.RDD
import RDDElementType._

case class AdjustedCountNGramRDD(adjustedCountNGramRDD: RDD[NGramCountType], config: SparkLMConfig) {
  val MAX_MOD_KN_SPECIAL_COUNT  = config.maxModKNSpecialCount

  def save: Either[String, Unit] = {
    try {
      Right(adjustedCountNGramRDD
        .map(nGram => s"${nGram._1.mkString(",")}\t${nGram._2}")
        .saveAsTextFile(config.adjustedCountOutputDir))
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  def collectCountOfCountList = {
    val countOfCount = adjustedCountNGramRDD.filter { case (nGram, adjustedCount) =>
      val order = nGram.length
      order > 0 &&  adjustedCount <= (MAX_MOD_KN_SPECIAL_COUNT + 1)
    }.map { case (nGram, adjustedCount) =>
      ((nGram.length, adjustedCount.toInt), 1L)
    }.reduceByKey(_+_).collect()

    countOfCount
  }

  def toUninterpolatedIntermediatedNGramRDD = {
    val minCountsPre = config.minCountsPre
    val maxOrder = config.nGramOrder

    val unIterpolatedNGramRDD = adjustedCountNGramRDD
      .filter(_._1.nonEmpty)
      .map{ case (nGram, adjustedCount) =>
        val order = nGram.length
        val (history, word) = nGram.splitAt(order-1)
        (history, (word.head, adjustedCount))
      }.groupByKey()

    UninterpolatedIntermediateNGramRDD(unIterpolatedNGramRDD, config)
  }

}