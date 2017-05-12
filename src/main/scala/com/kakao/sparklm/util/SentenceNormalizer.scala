package com.kakao.sparklm.util

/**
  * Sentence Normalizer
  */
object SentenceNormalizer {

  val INVALID_CHARACTERS = """[^a-zA-Z0-9가-힣 \+\-\*\.%&\$\t]"""
  val URL = """^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"""
  val upperCaseLetter = ('A' to 'Z')

  def apply(sent: String): String = {
    // remove url!
    val _result = sent.replaceAll(URL, " ")

    // remove special characters!
    val result = _result.replaceAll(INVALID_CHARACTERS, " ")

    // convert a lowercase letter
    result.toLowerCase
  }
}