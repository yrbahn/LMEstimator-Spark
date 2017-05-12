package com.kakao.sparklm.util

import java.io.{File, PrintWriter}
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.{Map, mutable}
import scala.io.Source

class DiscountMap(discountMap: mutable.Map[(Int, Int), Double]) {

  def getDiscount(order: Int, countCat: Int) : Double = {
    if (order == 0 || countCat == 0)
      0.0
    else {
      discountMap((order, countCat))
    }
  }

  def save(outputDir:String)(implicit sc: SparkContext) = {

    val fileName = s"$outputDir/${DiscountMap.DISCOUNT_MAP_FILENAME}"

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
        discountMap.foreach{ case ((order, countCateg), disc) =>
          outputStream.println(s"$order\t$countCateg\t$disc")
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
        discountMap.foreach{ case ((order, countCateg), disc) =>
          pw.println(s"$order\t$countCateg\t$disc")
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


object DiscountMap {

  val DISCOUNT_MAP_FILENAME = "discountMap.txt"

  def apply(countOfCountList: Array[((Int, Int), Long)], maxOrder: Int, maxSpecialCount: Int) = {
    val m = countOfCountList.toMap
    val discountMap = mutable.HashMap[(Int, Int), Double]()

    for(order <- 1 to maxOrder)
      for(countCateg <-1 to maxSpecialCount) {
        val disc = knDiscount(m, order, countCateg)
        discountMap((order, countCateg)) = disc
      }

    new DiscountMap(discountMap)
  }

  def load(config: SparkLMConfig)(implicit sc: SparkContext): Either[String, DiscountMap] = {
    try {
      val fileName = s"${config.outputDir}/${DiscountMap.DISCOUNT_MAP_FILENAME}"

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
          val discountMap = Source.fromInputStream(inputStream).getLines()
            .foldLeft(mutable.HashMap[(Int, Int), Double]()){ (m, line) =>
              val cols = line.split("\t")
              m((cols(0).toInt, cols(1).toInt)) = cols(2).toDouble
              m
            }

          //inputStream.close()
          Right(new DiscountMap(discountMap))
        } else
          Left("no file")
      } else {
        val countMap = mutable.HashMap[(Int, Int), Double]()
        countMap.withDefaultValue(0.0)

        val discountMap = scala.io.Source.fromFile(fileName)
          .getLines()
          .foldLeft(countMap){ (m, line) =>
            val cols = line.split("\t")
            m((cols(0).toInt, cols(1).toInt)) = cols(2).toDouble
            m
          }
        Right(new DiscountMap(discountMap))
      }
    } catch {
      case e : Throwable =>
        Left(e.getMessage)
    }
  }

  def knDiscount(countOfCount: Map[(Int, Int), Long], order: Int, countCat: Int) = {
    //val y : Double = countOfCount((order, 1))/ (countOfCount((order, 1)) + 2 * countOfCount((order, 2))).toDouble
    countCat - (((countCat + 1) *  countOfCount((order, 1)) * countOfCount((order, countCat + 1))).toDouble /
      ((countOfCount((order, 1)) + 2 * countOfCount((order, 2))) * countOfCount((order, countCat))).toDouble)
  }

}

