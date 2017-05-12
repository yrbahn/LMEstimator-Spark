package com.kakao.sparklm.util

import org.apache.spark.Partitioner
import scala.util.control.Breaks._

import com.kakao.sparklm.rdd.RDDElementType._

object NGramUtil{
  def replace(nGram: NGramType, sourceId: Int, replaceId: Int) = {
    nGram.map{ id =>
      if (id == sourceId)
        replaceId
      else
        id
    }
  }

  def compareNGram(left: NGramType, right: NGramType) : Int = {
    var result = 0
    val size1 = left.length
    val size2 = right.length
    val end = if (size1 >= size2) size1 else size2

    breakable {
      for (i <- 0 until end) {
        if (i >= size1) {
          result = -1
          break()
        } else if (i >= size2) {
          result = 1
          break()
        } else {
          val leftId = left(i)
          val rightId = right(i)
          if (leftId > rightId) {
            result = 1
            break()
          } else if (leftId < rightId) {
            result = -1
            break()
          }
        }
      }
    }

    result
  }

  class NGramPartitionerForBackOff(partitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val nGramKey = key.asInstanceOf[(NGramType, Int, Boolean)]
      val order = nGramKey._2
      val nGram = nGramKey._1
      val nGramOrder = nGram.length
      val includeLastToken = nGramOrder < order

      val hash1  = Hashing.smear(order)
      var hash2  = 0
      val end = if (includeLastToken) nGramOrder else nGramOrder -1
      for(i<-0 until end) {
        if (i == 0)
          hash2 = Hashing.smear(nGram.head)
        else
          hash2 ^= Hashing.smear(nGram(i) << (8*i))
      }

      (hash1 ^ hash2) % numPartitions
    }

    override def numPartitions: Int = partitions
  }

  class NGramPartitioner(partitions: Int, includeLastToken: Boolean=false,
                         maxOrder: Option[Int] = None) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val nGram = key.asInstanceOf[Vector[Int]]
      val end =
        if (maxOrder.isDefined)
          maxOrder.get
        else {
          if (includeLastToken)
            nGram.length
          else
            nGram.length - 1
        }

      var hash = 0

      for(i<-0 until end) {
        if (i == 0)
          hash = Hashing.smear(nGram.head)
        else
          hash ^= Hashing.smear(nGram(i) << (8*i))
      }

      hash % numPartitions
    }

    override def numPartitions: Int = partitions
  }

  implicit val nGramOrdering = new Ordering[NGramType] {
    override def compare(left: NGramType, right: NGramType) : Int = {
      compareNGram(left, right)
    }
  }

  implicit val nGramOrderingForBackOff = new Ordering[(NGramType, Int, Boolean)] {
    override def compare(left: (NGramType, Int, Boolean), right: (NGramType, Int, Boolean)) : Int = {
        var result = 0
        val nGramLeft = left._1
        val nGramLeftOrder = left._2

        val nGramRight = right._1
        val nGramRightOrder = right._2

        if (nGramLeftOrder < nGramRightOrder)
          result = -1
        else if (nGramLeftOrder > nGramRightOrder)
          result = 1
        else {
          val nGramResult = NGramUtil.compareNGram(nGramLeft, nGramRight)
          if (nGramResult != 0)
            result = nGramResult
          else {
            val isLast1: Boolean = left._3
            val isLast2: Boolean = right._3
            if (!isLast1 && isLast2)
              result = -1
            else if (isLast1 && !isLast2)
              result = 1
            else
              result = 0
          }
        }
        result
      }
  }

  object Hashing{

    def smear(hashCode: Int): Int = {
      var _hashCode = hashCode
      _hashCode ^= (_hashCode >>> 20) ^ (_hashCode >>> 12)
      _hashCode ^ (_hashCode >>> 7) ^ (_hashCode >>> 4)
    }
  }
}