package com.kakao.sparklm.util

object Sanity{

  def checkValidReal(real: Double) = {
    if (real.isInfinity || real.isNaN)
      throw new IllegalArgumentException("Not a valid real number: " + real)
  }

  def checkValidProb(prob: Double) = {
    checkValidReal(prob)
    if(prob < 0.0 || prob > 1.0)
      throw new IllegalArgumentException("Not a valid probability: " + prob)
  }

  def checkValidBackoff(bo: Double) = {
    checkValidReal(bo)
    if(bo < 0.0)
      throw new IllegalArgumentException("Not a valid backoff weight: " + bo)
  }
}