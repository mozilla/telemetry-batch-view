package com.mozilla.telemetry.experiments

import scala.util.hashing.MurmurHash3


object Permutations {
  def weightedGenerator(cutoffs: List[Double], salt: String, numPermutations: Int = 5000)
                       (id: String): Seq[Byte] = {
    val hashBy = id + salt
    val permutations = Array.fill[Byte](numPermutations)(0).zipWithIndex
    val cutoffsWithIndex = cutoffs.zipWithIndex.map {case (v, i) => (v, i.toByte)}

    permutations.map { case (_, index) => weightedChoice(cutoffsWithIndex, hashBy, index) }.toSeq
  }

  // cutoffs is a tuple with the index as Byte so we avoid doing this millions of times
  def weightedChoice(cutoffs: List[(Double, Byte)], hashBy: String, seed: Int): Byte = {
    // AND the hash to make sure you have a positive number, then divide by the max int value to get a Double
    val randish = (MurmurHash3.stringHash(hashBy, seed) & 0x7FFFFFFF) / Int.MaxValue.toDouble
    // Get the first element in the weights cdf where the randomish double is less than or equal to the cutoff value
    cutoffs.collectFirst { case(c, i) if randish <= c => i } match {
      case Some(b) => b
      case _ => throw new Exception("Couldn't find a matching cutoff: ensure cutoffs cover through 1 inclusive")
    }
  }
}
