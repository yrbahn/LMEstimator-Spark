package com.kakao.sparklm.util

import java.nio.charset.CodingErrorAction
import org.apache.spark.rdd.RDD

import scala.io.Codec


case class Preprocessor(lmConfig: SparkLMConfig) {

  def run(rows: RDD[String]) =
    try {
      // preprocessing and executing kg2p
      executeKG2P(preprocess(rows))
      //executeKG2P(rows)
    } catch {
      case e : Throwable =>
        throw new Exception(s"errors in K2P: s{e.getMessages}")
    }

  private def preprocess(rows: RDD[String]) = {
    val filteredCorpus = if (lmConfig.filterEnable) {
      val filterConfig = lmConfig.filterConfig

      val sentenceFilter = SentenceFilter(filterConfig.maxSentenceLen,
        filterConfig.minSentenceLen, filterConfig.maxWordLen, filterConfig.maxCombiWordCount)

      rows.filter { line =>
        val index = line.lastIndexOf('\t')
        if (index != -1) {
          val qc =
            try {
              line.substring(index + 1).trim.toInt
            } catch {
              case _ => 1
            }

          if (filterConfig.qcMaxCount == 0 || (qc > 0 && qc <= filterConfig.qcMaxCount))
            sentenceFilter.check(line.substring(0, index))
          else
            false

        } else
          sentenceFilter.check(line)
      }
    }
    else
      rows

    if(lmConfig.normalizationEnable)
      filteredCorpus.map{ line =>
        SentenceNormalizer(line)
      }
    else
      filteredCorpus
  }

  private def executeKG2P(rows: RDD[String]) : RDD[String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.IGNORE)
    rows.pipe(lmConfig.tokenizerPath)
  }

}