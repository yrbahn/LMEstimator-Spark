package sparklm.rdd

import sparklm.SparkLM
import sparklm.rdd.RDDElementType.InterpolatedNGramType
import sparklm.util.SparkLMConfig
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

case class BackOffRDD(backOffRDD: RDD[InterpolatedNGramType],
                      config: SparkLMConfig) {
  def saveTextFormat(unigramCntAccVar : LongAccumulator,
                     bigramCntAccVar  : LongAccumulator,
                     trigramCntAccVar : LongAccumulator,
                     idxToWordMap:Broadcast[Map[Int, String]])  = {

    try {
      backOffRDD.map { case (nGram, valueInfo) =>
        val (prob, bo) = valueInfo
        var logProb: Double = scala.math.log10(prob)
        if (logProb == Float.NegativeInfinity) {
          logProb = SparkLM.LOG_PROB_OF_ZERO
        }

        var logBackoff: Double = scala.math.log10(bo)
        if (bo == 0)
          logBackoff = SparkLM.LOG_PROB_OF_ZERO

        val order = nGram.length
        order match {
          case 1 =>
            unigramCntAccVar.add(1)
          case 2 =>
            bigramCntAccVar.add(1)
          case 3 =>
            trigramCntAccVar.add(1)
          case _ =>
        }

        if (config.arpaFormat) {
          val nGramStr = nGram.map { id => idxToWordMap.value(id) }.mkString(" ")
          (order, f"$logProb%1.6f $nGramStr $logBackoff%1.6f")
        } else {
          val nGramStr = nGram.map { id => f"$id%07d" }.mkString("\t")
          (order, f"$nGramStr\tP:$logProb%1.6f\tA:$logBackoff%1.6f")
        }
      }.saveAsHadoopFile(s"${config.outputDir}/7", classOf[String],
        classOf[String], classOf[RDDMultipleTextOutputFormat])
      Right()
    } catch {
      case e : Throwable => Left(e.getMessage)
    }
  }
}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.toString +"/"+ name
}
