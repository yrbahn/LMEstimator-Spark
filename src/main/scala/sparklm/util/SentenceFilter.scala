package sparklm.util

import scala.util.control.Breaks._

/**
  * Sentence Filter
  * @param maxSentLen
  * @param minSentLen
  * @param maxWordLen
  * @param maxCombiLen
  */
case class SentenceFilter(maxSentLen: Int, minSentLen:Int, maxWordLen: Int,
                          maxCombiLen: Int) {

  def check(sent: String): Boolean = {
    var combiCnt = 0
    var wordLen = 0
    var sentLen = 0

    var result = true
    breakable {
      sent.foreach { c =>
        if (sentLen > maxSentLen || wordLen > maxWordLen || combiCnt >= maxCombiLen) {
          result = false
          break()
        }
        else {
          combiCnt =
            if (c.isDigit || isASCII(c))
              combiCnt + 1
            else
              0
          if (c.isWhitespace) {
            wordLen = 0
          }
          else {
            wordLen += 1
          }

          sentLen += 1
        }
      }
    }

    if (result) {
      minSentLen < sentLen
    } else
      false
  }

  def isASCII(c: Char) : Boolean = {
    (c >= 'a' && c <= 'z') || (c >= 'A'  && c <= 'Z')
  }
}
