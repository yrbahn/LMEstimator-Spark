package sparklm.util

object KG2PLineParser{
  val usefulDigit = """^[0-9]{1,}\.{0,1}[0-9]{0,5}?(\^)?$""".r

  def createWordInfo(s: String) : String = {
    val wordInfo = s.split('\t')
    val morph = wordInfo(4)
    if(wordInfo.length == 5) {
      val word =
        if (morph == "ff")
          wordInfo(0).toLowerCase
        else
          wordInfo(0)

      if(word.indexOf(' ') != -1)
        s"""$morph:${word.replaceAll("\\s+", "_")}\t${wordInfo(2)}\t${wordInfo(3)}"""
      else
        s"""$morph:$word\t${wordInfo(2)}\t${wordInfo(3)}"""
    } else
      ""
  }

  /**
    *
    * @param s
    * @return (morph:word, context, pron)
    */
  def createWord(s: String) : String = {
    val wordInfo = s.split('\t')
    val morph = wordInfo(4)
    if(wordInfo.length == 5) {
      val word =
        if (morph == "ff")
          wordInfo(0).toLowerCase
        else
          wordInfo(0)

      if(word.indexOf(' ') != -1)
        s"""$morph:${word.replaceAll("\\s+", "_")}"""
      else
        s"""$morph:${word}"""
    } else
      ""
  }

  //return a morph of word from a word string
  def getMorph(wordInfoString: String) =  {
    val index = wordInfoString.lastIndexOf('\t')
    if(index != -1)
      wordInfoString.substring(index+1).trim
    else
      "--"
  }

  //
  def isValidWord(word: String, morph: String, bwList: BWList): Boolean = {
    val _word = removeLastCaret(word)
    morph match {
      case "gg"  => // digits
        usefulDigit.unapplySeq(_word).isDefined
      case "ff" => // english
        bwList.checkWhiteList(_word)
      case _ => // others
        !bwList.checkBlackList(_word)
    }
  }

  private def removeLastCaret(word: String) : String = {
    if(word.last == '^')
      word.dropRight(1)
    else
      word
  }
}
