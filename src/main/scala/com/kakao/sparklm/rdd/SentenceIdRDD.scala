package com.kakao.sparklm.rdd

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.rdd.RDDElementType.NGramCountType
import com.kakao.sparklm.util.SparkLMConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

case class SentenceIdRDD(sentenceIdRDD: RDD[(ListBuffer[Int], Int)], config: SparkLMConfig){

  def cache(storageType : StorageLevel = StorageLevel.MEMORY_AND_DISK_SER) = {
    sentenceIdRDD.persist(storageType)
  }

  def save: Either[String, Unit] = {
    try {
      Right(sentenceIdRDD.map(sent => s"""${sent._2} ${sent._1.mkString(" ")}""")
        .saveAsTextFile(config.preprocessedOutputDir))
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  /**
    * toNGramCountRDD
    * @param maxOrder
    * @return
    */
  def toNGramCountRDD(maxOrder: Int) : NGramCountRDD = {
    val nGramCountRDD = sentenceIdRDD.mapPartitions { sentIdIter =>
      var nGramList : List[NGramCountType] = List()
      sentIdIter.foreach{ case (sent, freqCnt) =>
        for(i<-1 to maxOrder){
          if (i == 1)
            nGramList = (Vector(SparkLM.BOS_ID), freqCnt) :: nGramList
          else {
            val extendedSent = SparkLM.BOS_ID +: sent :+ SparkLM.EOS_ID
            extendedSent.sliding(i).foreach{ nGram =>
              nGramList = (nGram.reverse.toVector, freqCnt) :: nGramList
            }
          }
        }
      }
      nGramList.iterator
    }

    cache(StorageLevel.DISK_ONLY)

    val _nGramCountRDD = nGramCountRDD.reduceByKey(_+_)
    NGramCountRDD(_nGramCountRDD, config)
  }

}



