package telemetry.test

import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import telemetry.streams.main_summary.Utils

class ChurnTest extends FlatSpec with Matchers{
  val testHistogram = """
{
 "payload": {
  "histograms": {
   "TEST_BOOLEAN_1": {
    "sum_squares_hi": 0,
    "values": {
      "1": 1,
      "0": 0
    },
    "histogram_type": 2,
    "bucket_count": 3,
    "sum_squares_lo": 0,
    "range": [1, 2],
    "sum": 1
   },
   "TEST_BOOLEAN_2": {
    "sum_squares_hi": 0,
    "values": {
      "1": 0,
      "0": 1
    },
    "histogram_type": 2,
    "bucket_count": 3,
    "sum_squares_lo": 0,
    "range": [1, 2],
    "sum": 0
   },
   "TEST_BOOLEAN_3": {
    "sum_squares_hi": 0,
    "values": {
      "1": 0,
      "0": 0
    },
    "histogram_type": 2,
    "bucket_count": 3,
    "sum_squares_lo": 0,
    "range": [1, 2],
    "sum": 0
   },
   "TEST_ENUM_1": {
    "sum_squares_hi": 0,
    "values": {
      "1": 21,
      "0": 0,
      "2": 0
    },
    "histogram_type": 1,
    "bucket_co\nunt": 9,
    "sum_squares_lo": 21,
    "range": [1, 8],
    "sum": 21
   },
   "TEST_ENUM_2": {
    "sum_squares_hi": 0,
    "values": {
      "42": 1,
      "47": 8,
      "48": 0,
      "36": 3,
      "100": 3,
      "35": 0
    },
    "hist\nogram_type": 1,
    "bucket_count": 101,
    "sum_squares_lo": 23324,
    "range": [1, 100],
    "sum": 627
   }
  }
 }
}
"""
  "A boolean histogram" can "be converted to a boolean" in {
    val json = parse(testHistogram)
    val histograms = json \ "payload" \ "histograms"

    Utils.booleanHistogramToBoolean(histograms \ "TEST_BOOLEAN_1").get should be (true)
    Utils.booleanHistogramToBoolean(histograms \ "TEST_BOOLEAN_2").get should be (false)
    Utils.booleanHistogramToBoolean(histograms \ "TEST_BOOLEAN_3") should be (None)
    Utils.booleanHistogramToBoolean(histograms \ "NO_SUCH_HISTOGRAM") should be (None)
  }

  "An enum histogram" can "be converted to a number" in {
    val json = parse(testHistogram)
    val histograms = json \ "payload" \ "histograms"

    Utils.enumHistogramToCount(histograms \ "TEST_ENUM_1").get should be (1)
    Utils.enumHistogramToCount(histograms \ "TEST_ENUM_2").get should be (100)
    Utils.enumHistogramToCount(histograms \ "NO_SUCH_HISTOGRAM") should be (None)
  }
}
