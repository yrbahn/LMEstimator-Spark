package com.kakao.sparklm.rdd

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.rdd.RDDElementType.NGramCountType
import com.kakao.sparklm.util.SparkLMConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.SortedSet

case class SentenceRDD(sentenceRDD: RDD[(ListBuffer[String], Int)], config: SparkLMConfig){

  def cache(storageType : StorageLevel = StorageLevel.MEMORY_AND_DISK_SER) = {
    sentenceRDD.persist(storageType)
  }

  private[this] def convertMultiPronFormat(s: String) : String = {
    val i = s.indexOf('^')
    if (i != -1) {
      val pronAndCtxts = s.split('\t')
      if (pronAndCtxts.length == 2) {
        val prons = pronAndCtxts(0).split('^')
        val ctxts = pronAndCtxts(1).split('^')
        prons.zip(ctxts).map(s => s"""${s._1}\t${s._2}""").mkString("\t")
      } else
        s
    } else {
      s
    }
  }

  def save: Either[String, Unit] = {
    try {
       Right(sentenceRDD.map(sent => s"""${sent._1.mkString(" ")}\t${sent._2}""")
          .saveAsTextFile(config.preprocessedOutputDir))
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  def toWordList: ArrayBuffer[String] = {
    if (config.tokenizer == "kg2p") {

      val wordRDD = sentenceRDD.flatMap { sent =>
        sent._1.map(token => (token, 1))
      }.map { case (w, freqCnt) =>
        val index = w.indexOf('\t')
        if (index != -1) {
          (s"${w.substring(0, index)}", (SortedSet(s"${w.substring(index + 1)}"), freqCnt))
        }
        else
          ("", (SortedSet[String](), freqCnt))
      }.filter(_._1.nonEmpty).reduceByKey { (left, right) =>
        val elem1 = right._1 ++ left._1
        val elem2 = right._2 + left._2
        (elem1, elem2)
      }

      val orderingDesc =
        Ordering.by[(String, (SortedSet[String], Int)), (Int, String)]( e => (e._2._2, e._1))

      wordRDD //take(dictConfig.dictSize-2)
        .top(config.dictWordCount - 2)(orderingDesc)
        .sortBy(_._1)
        .map { w =>
          val pronAndCtxtSet = w._2._1
          val wordKey = w._1
          val pronAndCtxtStr = pronAndCtxtSet.map(convertMultiPronFormat(_)).mkString("\t")
          s"""$wordKey\t$pronAndCtxtStr"""
        }.to[ArrayBuffer]
    } else {

      val wordRDD = sentenceRDD.flatMap { sent =>
        sent._1.map(token => (token, sent._2))
      }

      val orderingDesc = Ordering.by[(String, Int), (Int, String)]( e => (e._2, e._1))

      wordRDD
        .reduceByKey(_ + _)
        .top(config.dictWordCount - 2)(orderingDesc)
        .sortBy(_._1)
        .map(_._1)
        .to[ArrayBuffer]
    }
  }


  def toSentenceIdRDD(maxOrder: Int, bcWord2Idx:Broadcast[Map[String, Int]]) : SentenceIdRDD = {

    val sentenceIdRDD = sentenceRDD.mapPartitions{ sentenceIter =>
      var ngramList : List[NGramCountType] = Nil
      val word2Idx = bcWord2Idx.value
      val isKG2P = config.tokenizer == "kg2p"

      if (isKG2P) {
        val unknownId =  bcWord2Idx.value(SparkLM.UNK_STR2)
        sentenceIter.map { case (sent, qc) =>
          (sent.map { wordInfo =>
            val word = wordInfo.split('\t').head
            word2Idx.getOrElse(word, unknownId)
          }, qc)
        }
      } else {
        val unknownId = bcWord2Idx.value(SparkLM.UNK_STR1)
        sentenceIter.map { case (sent, qc) =>
          (sent.map(word2Idx.getOrElse(_, unknownId)), qc)
        }
      }
    }.filter(_._1.nonEmpty)

    SentenceIdRDD(sentenceIdRDD, config)
  }

}



