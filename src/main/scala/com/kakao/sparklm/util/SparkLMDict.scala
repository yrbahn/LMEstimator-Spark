package com.kakao.sparklm.util

import java.io.{File, PrintWriter}
import java.net.URI

import com.kakao.sparklm.SparkLM
import com.kakao.sparklm.rdd.{RawSentenceRDD, SentenceRDD}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.immutable.Map
import scala.io.Source

object SparkLMDict{

  def create(config: SparkLMConfig, sentenceRDD: SentenceRDD): Either[String, Seq[String]] = {
    //creating a dictionary
    try {
      val wordList = sentenceRDD.toWordList

      val addedWordList =
        if (config.tokenizer == "kg2p")
          SparkLM.SIL +: SparkLM.EOS +: SparkLM.BOS +: wordList :+ s"${SparkLM.UNK_STR2}\t${SparkLM.UNK_PRN2}"
        else
          SparkLM.SIL +: SparkLM.EOS +: SparkLM.BOS +: wordList :+ SparkLM.UNK_STR1

      Right(addedWordList)
    } catch {
      case  e : Throwable =>
        Left(e.getMessage)
    }
  }

  def load(config: SparkLMConfig, prndictFile:Option[String]=None)(implicit sc: SparkContext): Either[String, Seq[String]] = {
    try {
      val fileName = prndictFile.getOrElse(s"${config.outputDir}/${config.dictFileName}")

      if (fileName.trim.startsWith("hdfs://")) {
        val conf = sc.hadoopConfiguration
        val uri = new URI(fileName)
        val hadoopHost =
          if (uri.getPort() != -1)
            s"${uri.getScheme}://${uri.getHost}:${uri.getPort}"
          else
            s"${uri.getScheme}://${uri.getHost}"

        conf.set("fs.defaultFS", hadoopHost)

        val fs = FileSystem.get(conf)
        val fileNamePath = new Path(uri.getPath)
        if (fs.exists(fileNamePath)) {
          val inputStream = fs.open(fileNamePath)
          val wordDict =
            Source.fromInputStream(inputStream).getLines().map{ w =>
              val index = w.indexOf('\t')
              if (index != -1)
                w.substring(0, index)
              else
                w.trim()
            }.toSeq
          //inputStream.close()
          Right(wordDict)
        } else
          Left("no file")
      } else {
        Right(scala.io.Source.fromFile(fileName)
          .getLines().map{ w =>
          val index = w.indexOf('\t')
          if (index != -1)
            w.substring(0, index)
          else
            w.trim()
        }.toSeq)

      }
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  def word2Idx(wordList:Seq[String], arpaFormat: Boolean = false) : Map[String, Int] =  {
    if (arpaFormat)
      wordList.zipWithIndex.toMap
    else
      wordList.map(_.split('\t')(0)).zipWithIndex.toMap
  }

  def idx2word(word2Idx: Map[String, Int]) =
    word2Idx.map(_.swap)

  def idx2word(wordList:Seq[String]) : Map[Int, String] = {
    word2Idx(wordList).map(_.swap)
  }

  def getUnkId(word2Idx:Map[String, Int], arpaFormat: Boolean = false) =
    if (arpaFormat)
      word2Idx.getOrElse(SparkLM.UNK_STR1, word2Idx.size)
    else
      word2Idx.getOrElse(SparkLM.UNK_STR2, word2Idx.size)

  def save(config: SparkLMConfig, wordList: Seq[String], dir: String)(implicit sc: SparkContext): Either[String, Unit] = {
    val fileName = s"$dir/${config.dictFileName}"

    if (fileName.trim.startsWith("hdfs://")){
      val conf = sc.hadoopConfiguration
      val uri = new URI(fileName)
      val hadoopHost =
        if (uri.getPort() != -1)
          s"${uri.getScheme}://${uri.getHost}:${uri.getPort()}"
        else
          s"${uri.getScheme}://${uri.getHost}"

      conf.set("fs.defaultFS", hadoopHost)

      val fs = FileSystem.get(conf)

      val filenamePath = new Path(uri.getPath)
      try {
        if (fs.exists(filenamePath))
          fs.delete(filenamePath, true)

        val outputStream = new java.io.PrintWriter(fs.create(filenamePath))
        //write words

        wordList.foreach {
          word => outputStream.println(word)
        }

        outputStream.close()

        Right()
      } catch {
        case e: Throwable =>
          Left(e.getMessage)
      }
    } else {
      try {
        val pw = new PrintWriter(new File(fileName))
        wordList.foreach { case word =>
          pw.println(s"$word")
        }

        pw.close()
        Right()
      } catch{
        case e : Throwable =>
          Left(e.getMessage)
      }
    }
  }


}
