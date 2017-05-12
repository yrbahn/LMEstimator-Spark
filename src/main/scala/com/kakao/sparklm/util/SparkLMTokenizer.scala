package com.kakao.sparklm.util

import java.nio.charset.CodingErrorAction

import com.kakao.sparklm.SparkLM
import com.twitter.penguin.korean.TwitterKoreanProcessor
import com.twitter.penguin.korean.util.KoreanPos

import scala.io.Codec

trait SparkLMTokenizer

object SparkLMTokenizer{
  def normalize(text: CharSequence) : CharSequence= {
    TwitterKoreanProcessor.normalize(text)
  }

  def tokenize(text: CharSequence) : Seq[String] = {
    val tokens = TwitterKoreanProcessor.tokenize(text)

    //filter
    val filteredTokens = tokens.filter{ token =>
      token.pos != KoreanPos.Punctuation &&
      token.pos != KoreanPos.Space &&
      token.pos != KoreanPos.URL &&
      token.pos != KoreanPos.Others &&
      token.length < 15
    }

    TwitterKoreanProcessor.tokensToStrings(filteredTokens)
  }

  def apply(text: String) = {
    val normalizedText = normalize(text)
    tokenize(normalizedText)
  }
}
