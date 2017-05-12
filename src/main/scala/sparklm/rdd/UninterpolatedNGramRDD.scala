package sparklm.rdd

import sparklm.SparkLM
import sparklm.rdd.RDDElementType.{NGramType, UniterpolatedNGramType}
import sparklm.util.NGramUtil.NGramPartitioner
import sparklm.util.NGramUtil.nGramOrdering
import sparklm.util.SparkLMConfig
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.collection.Map

case class UninterpolatedNGramRDD(uninterpolatedNGramRDD: RDD[UniterpolatedNGramType],
                                  config : SparkLMConfig){
  def save(implicit sc: SparkContext) = {
    try {
      Right(uninterpolatedNGramRDD.map{uninterp =>
        s"${uninterp._1.mkString(",")}\t${uninterp._2}"}
        .saveAsTextFile(config.uninterpolatedProbOutputDir))

    } catch {
      case e : Throwable => Left( e.getMessage() + ": cannot save uninterpNGramRDD")
    }
  }

  def getUniGrams = {
    uninterpolatedNGramRDD.filter { uniterpNGram =>
      val historyNGRam = uniterpNGram._1
      val order = historyNGRam.length + 1
      order == 1
    }.map { uniterpUniGram =>
      val (historyNGram, value) = uniterpUniGram
      val nGram = historyNGram :+ value._1
      (nGram, (value._2, value._3))
    }.collectAsMap()
  }

  def toInterpolationNGramRDD(uninterpUnigramMap: Broadcast[Map[NGramType, (Double, Double)]]) = {
    val maxOrder = config.nGramOrder

    val interpolatedNGramRDD = uninterpolatedNGramRDD.filter { uninterpNGram =>
      val historyNGRam = uninterpNGram._1
      val order = historyNGRam.length + 1
      order > 1
    }.map { uniterpNGram =>
      val (historyNGram, value) = uniterpNGram
      val nGram = historyNGram :+ value._1
      (nGram.reverse, (value._2, value._3))
    }.repartitionAndSortWithinPartitions(new NGramPartitioner(uninterpolatedNGramRDD.getNumPartitions, maxOrder = Some(2)))
      .mapPartitionsWithIndex { (i, uniterpolatedNGramIter) =>
      val (zeroGramUninterpProb, _) = uninterpUnigramMap.value(Vector[Int](SparkLM.ZEROTON_ID))
      val prob = ListBuffer.fill[Double](maxOrder+1)(0)
      val result = ListBuffer[(NGramType, (Double, Double))]()

      uniterpolatedNGramIter.foreach { case (reversedNGram, value) =>
        val nGram = reversedNGram.reverse
        val order = nGram.length
        val (uninterpProb, interpWeight) = value

        prob(order) = uninterpProb

        if (order == 2){
          val uniGram = nGram.tail
          val (uniGramUninterpProb, uniGramInterpWeight) = uninterpUnigramMap.value(uniGram)

          // b()*(1/voca_size)
          val preProb = uniGramUninterpProb + uniGramInterpWeight * zeroGramUninterpProb
          prob(order) = uninterpProb + interpWeight * preProb
        } else if (order > 2){
          prob(order) = uninterpProb + interpWeight * prob(order-1)
        }

        if (order > 1)
          result.append((nGram, (prob(order), interpWeight)))
      }

      if (i == 0) { // emmit once because of duplicated unigram
        uninterpUnigramMap.value.foreach{ case (uniGram, value) =>
          if (uniGram.head != SparkLM.ZEROTON_ID) {
            val (uniGramUninterpProb, uniGramInterpWeight) = value
            val preProb = uniGramUninterpProb + uniGramInterpWeight * zeroGramUninterpProb
            result.append((uniGram, (preProb, uniGramUninterpProb)))
          }
        }
      }

      result.iterator
    }

    InterpolatedNGramRDD(interpolatedNGramRDD, config)
  }
}
