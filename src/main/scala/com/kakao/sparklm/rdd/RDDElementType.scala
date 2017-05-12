package com.kakao.sparklm.rdd

object RDDElementType {
  type NGramType = Vector[Int]
  type NGramCountType = (NGramType, Int)
  type UniterpolatedIntermediateNGramType = (NGramType, Iterable[(Int, Int)])
  type UniterpolatedNGramType = (NGramType, (Int, Double, Double))
  type InterpolatedNGramType = (NGramType, (Double, Double))
}

