package sparklm.util

import sparklm.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


object DataSource{

  def toRawSentenceRDD(config: SparkLMConfig)
                   (implicit sc: SparkContext): RawSentenceRDD = {

    val rawSentenceRDD = sc.textFile(config.inputs.mkString(","))

    RawSentenceRDD(rawSentenceRDD, config)
  }

  def loadSentenceIdRDD(config: SparkLMConfig)(implicit sc:SparkContext) = {

    val sentenceIdRDD : RDD[(ListBuffer[Int], Int)] =
      sc.textFile(config.preprocessedOutputDir).map { line =>
        val sent = line.split(" ")
        (sent.tail.map(_.toInt).to[ListBuffer], sent.head.toInt)
      }

    SentenceIdRDD(sentenceIdRDD, config)
  }


  def loadNGramCountRDD(config: SparkLMConfig)(implicit sc:SparkContext) = {

    val ngramCountRDD  =
      sc.textFile(config.ngramCountOutputDir).map { _line =>
        val line = _line.split("\t")
        if (line.length == 2){
          val nGramString = line.head.trim()
          val nGram =
            if(nGramString.isEmpty){
              Vector[Int]()
            } else {
              nGramString.split(",")
                .map(_.trim.toInt).toVector
            }
          (nGram, line(1).trim().toInt)
        } else {
          (Vector[Int](), 0)
        }
      }

    NGramCountRDD(ngramCountRDD, config)
  }

  def loadAdjustedCountNGramRDD(config: SparkLMConfig)(implicit sc:SparkContext) = {

    val adjustedCountNGramRDD =
      sc.textFile(config.adjustedCountOutputDir).map{ _line =>
        val line = _line.split("\t")
        if (line.length == 2){
          val nGramString = line.head.trim()
          val nGram =
            if(nGramString.isEmpty){
              Vector[Int]()
            } else {
              nGramString.split(",")
                .map(_.trim.toInt).toVector
            }
          (nGram, line(1).trim().toInt)
        } else {
          (Vector[Int](), 0)
        }
      }

    AdjustedCountNGramRDD(adjustedCountNGramRDD, config)
  }

  def loadUninterpolatedNGramRDD(config: SparkLMConfig)(implicit sc:SparkContext) = {

    val uninterpNGramRDD =
      sc.textFile(config.uninterpolatedProbOutputDir).map{ line =>
        val sLine = line.split("\t")
        val _nGram = sLine.head.split(",")
        val nGram =
          if (_nGram.length == 1){
            if (_nGram.head.trim.isEmpty)
              Vector[Int]()
            else
              Vector[Int](_nGram.head.trim.toInt)
          } else
            _nGram.map(_.trim.toInt).toVector

        val value = sLine(1).trim.drop(1).dropRight(1).split(",")
        (nGram, (value(0).toInt, value(1).toDouble, value(2).toDouble))
      }

    UninterpolatedNGramRDD(uninterpNGramRDD, config)
  }

  def loadInterpolatedNGramRDD(config: SparkLMConfig)(implicit sc:SparkContext) = {
    val interpNGramRDD =
      sc.textFile(config.interpolatedProbOutputDir).map{ line =>
        val sLine = line.split("\t")
        val _nGram = sLine.head.split(",")
        val nGram =
          if (_nGram.length == 1){
            if (_nGram.head.trim.isEmpty)
              Vector[Int]()
            else
              Vector[Int](_nGram.head.trim.toInt)
          } else
            _nGram.map(_.trim.toInt).toVector

        val value = sLine(1).trim.drop(1).dropRight(1).split(",")
        (nGram, (value(0).toDouble, value(1).toDouble))
      }

    InterpolatedNGramRDD(interpNGramRDD, config)
  }
}
