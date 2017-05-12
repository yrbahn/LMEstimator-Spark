package com.kakao.sparklm.rdd

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.util.NGramUtil.NGramPartitionerForBackOff
import com.kakao.sparklm.util.NGramUtil.nGramOrderingForBackOff
import com.kakao.sparklm.util.NGramUtil.nGramOrdering
import com.kakao.sparklm.util.{Sanity, SparkLMConfig}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import RDDElementType._

case class InterpolatedNGramRDD(interpolatedNGramRDD: RDD[InterpolatedNGramType],
                                  config: SparkLMConfig) {

  def save(implicit sc: SparkContext) = {
    try {
     Right(interpolatedNGramRDD.map{interp =>
       s"${interp._1.mkString(",")}\t${interp._2}"}.saveAsTextFile(config.interpolatedProbOutputDir))

    } catch {
      case _ : Throwable => Left("cannot save interpIntermediateNGramRDD")
    }
  }

  def toBackOffNGramRDD(unkId: Int) = {
    val maxOrder = config.nGramOrder
    val backOffRDD = interpolatedNGramRDD.flatMap{ case (nGram, value) =>
      val reversedNGram = nGram.reverse
      val order  = reversedNGram.length
      val (prob, _) = value
      val result = ListBuffer(((reversedNGram, order, true), (SparkLM.ZEROTON_ID, prob)))
      if(order > 0 && order <= maxOrder) {
        val (currentWordIdSeq, reversedContextIds) = reversedNGram.splitAt(1)
        val currentWordId = currentWordIdSeq.head
        val outputValue = (currentWordId, prob)
        if (order > 1) {
          result.append(((reversedContextIds, order-1, false), outputValue))
        }

        if(order < maxOrder){
          result.append(((reversedContextIds, order, false), outputValue))
        }
      }

      result
    }.repartitionAndSortWithinPartitions(new NGramPartitionerForBackOff(interpolatedNGramRDD.getNumPartitions*3))
      .mapPartitions { backOffNGramIter =>
        var zerotonProb = 1.0
        var subtractBuffer = 0.0
        var backOff = 1.0
        var denom = 1.0
        val denomMap = mutable.HashMap[Int, Double]()
        var numer = 1.0
        val result = ListBuffer[(NGramType, (Double, Double))]()
        var preLowerContext: NGramType = Vector[Int]()
        var preNumerator: NGramType = Vector[Int]()

        backOffNGramIter.foreach{ case (nGramKey, valueInfo) =>
          val (reversedContextIds, targetOrder, isLast) = nGramKey
          val (wordId, prob) = valueInfo
          val lowerOrder = reversedContextIds.length
          val isLowerOrderContext = lowerOrder < targetOrder && !isLast
          val isNumeratorKey = !isLast && !isLowerOrderContext

          if(isLowerOrderContext){ //cals denom
            if (preLowerContext != reversedContextIds){
              denomMap.clear()
            }

            if(targetOrder == 1 && wordId != SparkLM.ZEROTON_ID){
              if(prob >  1e-3){
                zerotonProb -= prob
              } else {
                subtractBuffer += prob
              }

              if(subtractBuffer > 1e-3) {
                zerotonProb -= subtractBuffer
                subtractBuffer = 0.0
              }
            }

            denomMap.update(wordId, prob)

            preLowerContext = reversedContextIds

          } else {
            if (isNumeratorKey){ // cals numerator
              if (preNumerator != reversedContextIds){
                if(preLowerContext != reversedContextIds.dropRight(1)){
                  denomMap.clear()
                  SparkLM.log.error("no denominator")
                  //throw an exception
                }

                denom = 1.0
                numer = 1.0
              }

              numer -= prob
              try {
                denom -= denomMap(wordId)
              } catch {
                case e : Throwable =>
                  SparkLM.log.error(s"${reversedContextIds.mkString(" ")} $wordId")
                  throw e
              }

              preNumerator = reversedContextIds

            } else { // emit backoff
              if (preNumerator != reversedContextIds){
                backOff = 1.0
              }else {
                if (numer < 0) {
                  numer = 0.0
                }

                if (denom < 0) {
                  denom = 0.0
                }

                if (denom > 0)
                  backOff = numer / denom
                else
                  backOff = 0.0
              }

              if (lowerOrder == 1 && reversedContextIds.head == unkId) {
                zerotonProb -= subtractBuffer
                result.append((reversedContextIds, (zerotonProb, backOff)))
              } else {
                Sanity.checkValidProb(prob)
                Sanity.checkValidBackoff(backOff)
                result.append((reversedContextIds.reverse, (prob, backOff)))
              }
            }
          }

        }

        result.iterator
      }.sortByKey()

      BackOffRDD(backOffRDD, config)
  }
}
