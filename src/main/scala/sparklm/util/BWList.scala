package sparklm.util

import scala.collection.immutable.HashSet
import scala.io.Source

/**
  * Black and White List
  * @param whiteList
  * @param blackList
  */
class BWList(whiteList:HashSet[String], blackList: HashSet[String]) {
  def checkWhiteList(w:String) = whiteList.contains(w)
  def checkBlackList(w:String) = blackList.contains(w)
}

object BWList {
  def apply(whiteListPath:Option[String], blackListPath: Option[String]): Option[BWList] = {
    try {
      for {
        whiteList <- whiteListPath.map(path => loadDictionary(path))
        blackList <- blackListPath.map(path => loadDictionary(path))
      } yield {
        new BWList(whiteList, blackList)
      }
    } catch {
      case _ : Throwable =>
        None
    }
  }

  def loadDictionary(path:String) : HashSet[String] = {
    val lines = (if(path.startsWith("http")) {
      Source.fromURL(path).getLines()
    } else {
      Source.fromFile(path).getLines()
    }).filter(s => !s.trim.isEmpty && !s.startsWith("//") )
      .map(_.split('\t')(0).trim())

    lines.foldLeft(HashSet[String]())(_+_)
  }
}

