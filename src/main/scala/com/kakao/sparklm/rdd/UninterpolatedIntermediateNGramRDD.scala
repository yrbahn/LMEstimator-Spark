package com.kakao.sparklm.rdd

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.rdd.RDDElementType.{NGramType, UniterpolatedIntermediateNGramType}
import com.kakao.sparklm.util.{DiscountMap, Sanity, SparkLMConfig}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

case class UninterpolatedIntermediateNGramRDD(uninterpolatedIntermediateNGramRDD: RDD[UniterpolatedIntermediateNGramType],
                                              config: SparkLMConfig){
  def save(implicit sc: SparkContext) = {
    try {
      Right(uninterpolatedIntermediateNGramRDD.map{uninterp =>
        s"${uninterp._1.mkString(",")}\t${uninterp._2.mkString(" ")}"}
        .saveAsTextFile(config.uninterpolatedProbOutputDir))

    } catch {
      case _ : Throwable => Left("cannot save uninterpIntermediateNGramRDD")
    }
  }

  def toUninterpolatedNGramRDD(discountMapBC: Broadcast[DiscountMap],
                               maxModKNSpecialCount: Int = config.maxModKNSpecialCount) = {

    val uniterpolatedNGramRDD = uninterpolatedIntermediateNGramRDD.flatMap { case (historyNGram, values) =>
      var numExtension = 0
      var fSum: Long = 0
      var backoffDenom: Long = 0
      val lowerOrder = historyNGram.length
      val g = ListBuffer.fill[Long](maxModKNSpecialCount)(0)
      val extensions = ListBuffer[(Int, Int)]()

      values.foreach { valueInfo =>
        val (wordId, adjustedCount) = valueInfo
        if (wordId != SparkLM.BOS_ID) {
          numExtension += 1
          fSum += adjustedCount

          val countCateg = getCountCategory(adjustedCount, maxModKNSpecialCount)
          g(countCateg - 1) = g(countCateg -1) + 1L

          backoffDenom += adjustedCount
        }
        extensions.append(valueInfo)
      }

      val interpolationWeight =
        calculateInterpolationWeight(historyNGram, backoffDenom, lowerOrder, g,
          numExtension, maxModKNSpecialCount, discountMapBC.value)

      if (lowerOrder == 0) {
        extensions.append((SparkLM.ZEROTON_ID, numExtension))
      }

      extensions.map { case (wordId, adjustedCount) =>
        if (lowerOrder == 0 && wordId == SparkLM.ZEROTON_ID){
          (historyNGram, (wordId, 1.0 / adjustedCount, interpolationWeight))
        } else if (wordId != SparkLM.BOS_ID) {
          val countCateq = getCountCategory(adjustedCount, maxModKNSpecialCount)
          val d = discountMapBC.value.getDiscount(lowerOrder+1, countCateq)
          //val d = 0.5
          val uninterpProb = (adjustedCount - d)/ fSum.toDouble

          //Check prob value
          Sanity.checkValidProb(uninterpProb)

          (historyNGram, (wordId, uninterpProb, interpolationWeight))
        } else {
          (historyNGram, (wordId, 0.0, 0.0))
        }
      }
    }

    UninterpolatedNGramRDD(uniterpolatedNGramRDD, config)
  }

  def calculateInterpolationWeight(historyNGram:NGramType, interpDenom: Long, lowerOrder: Int, g: ListBuffer[Long],
                                   numExtension: Long, maxModKNCount: Int, discountMap: DiscountMap) = {
    var numerator : Double = 0
    val discountOrder  = lowerOrder + 1
    for (iCountCateg <- 1 to maxModKNCount){
      numerator += discountMap.getDiscount(discountOrder, iCountCateg) * g(iCountCateg-1)
    }
    //numerator = 0.5 * numExtension

    if (numExtension == 0)
      1.0
    else
      numerator / interpDenom.toDouble
  }

  def getCountCategory(adjustedCount: Long, maxModKNSpecialCount: Int) = {
    Math.min(adjustedCount, maxModKNSpecialCount).toInt
  }
}