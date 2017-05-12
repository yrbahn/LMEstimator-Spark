package com.kakao.sparklm.rdd

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

case class RawSentenceRDD(rawSentenceRDD: RDD[String], config: SparkLMConfig){
  def preprocess(bwList: Broadcast[BWList]) : SentenceRDD = {
    val preprocessor = Preprocessor(config)

    val preprocessedSentenceRDD  =
      if (config.tokenizer == "kg2p") { // kg2p
        preprocessor.run(rawSentenceRDD).mapPartitions{ iter =>
          val result   = ListBuffer[(ListBuffer[String], Int)]()
          var sentence = ListBuffer[String]()
          var queryCnt  = 1

          iter.foreach { line =>
            val wordInfo = line.split("\\s+")

            wordInfo.head match {
              case SparkLM.BOS =>
                queryCnt =
                  try {
                    if (config.ignoreQueryCount)
                      1
                    else
                      wordInfo(1).toInt
                  } catch {
                    case _: Throwable => 1
                  }
                sentence = ListBuffer[String]()

              case SparkLM.EOS => // end of a sentence
                if (sentence.isDefinedAt(1)) {
                  result.append((sentence, queryCnt))
                }

              case SparkLM.SPACE_TAG =>
              // pass

              case word => // word
                val morph = KG2PLineParser.getMorph(line)
                if (morph != "--") {
                  if (KG2PLineParser.isValidWord(word, morph, bwList.value)) {
                    val wordInfo = KG2PLineParser.createWordInfo(line)
                    if (!wordInfo.isEmpty)
                      sentence += wordInfo
                  }
                }
            }
          }

          result.iterator
        }
      } else { // twitter
        rawSentenceRDD
          .mapPartitions { rowIterator =>
            rowIterator.map { row =>
              val sent = row.split( """\t""")
              val qc =
                if (sent.length > 1) {
                  try {
                    sent(1).toInt
                  } catch {
                    case _: java.lang.NumberFormatException =>
                      1
                  }
                } else
                  1
              val tokenSentence = SparkLMTokenizer(sent.head)
              (tokenSentence.to[ListBuffer], qc)
            }
          }
      }

    SentenceRDD(preprocessedSentenceRDD, config)
  }
}



