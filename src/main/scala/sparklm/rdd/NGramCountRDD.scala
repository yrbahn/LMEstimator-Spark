package sparklm.rdd

import sparklm.SparkLM
import sparklm.util.SparkLMConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import RDDElementType._
import sparklm.util.NGramUtil.NGramPartitioner
import sparklm.util.NGramUtil.nGramOrdering

case class NGramCountRDD(nGramRDD: RDD[NGramCountType], config: SparkLMConfig){
  def save(implicit sc: SparkContext):Either[String,Unit] = {
    try {
      Right(nGramRDD.map(nGram => s"${nGram._1.mkString(",")}\t${nGram._2}")
        .saveAsTextFile(config.ngramCountOutputDir))
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  /**
    * Calcuates Adjusted Counts
    * @param maxOrder
    * @return
    */
  def adjustedCount(maxOrder: Int) = {
    val assignedCountNGram = nGramRDD.repartitionAndSortWithinPartitions(new NGramPartitioner(nGramRDD.getNumPartitions))
      .mapPartitions{ nGramIter =>
        val previousReversedContext : ListBuffer[NGramType] = ListBuffer.fill(maxOrder)(Vector[Int]())
        val contextSum : ListBuffer[Int] = ListBuffer.fill(maxOrder)(0)
        //var result : List[NGramCountType] = List()
        val result : ListBuffer[NGramCountType] = ListBuffer()

        nGramIter.foreach { case (nGram, freqCnt) =>
          val order = nGram.length
          if (order == maxOrder || nGram(order-1) == SparkLM.BOS_ID){
            result.append((nGram.reverse, freqCnt))
          }

          val reversedContext = nGram.dropRight(1)
          val contextOrder    = order - 1
          if (previousReversedContext(contextOrder).isEmpty){
            previousReversedContext(contextOrder) = reversedContext
          } else {
            if (previousReversedContext(contextOrder) != reversedContext) {
              result.append((previousReversedContext(contextOrder), contextSum(contextOrder)))

              previousReversedContext(contextOrder) = reversedContext
              contextSum(contextOrder) = 0
            }
          }

          contextSum(contextOrder) += freqCnt
        }

        for(i <- 0 until maxOrder){
          if(contextSum(i) > 0){
            result.append((previousReversedContext(i).reverse, contextSum(i)))
          }
        }

        result.iterator
      }

      AdjustedCountNGramRDD(assignedCountNGram, config)
  }
}
