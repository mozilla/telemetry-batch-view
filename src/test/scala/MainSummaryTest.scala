package telemetry.test

import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}
import streams.MainSummary

class MainSummaryTest extends FlatSpec with Matchers{
  val testPayload = """
{
 "environment": {
  "addons": {
   "activeAddons": {
    "addon 1": {},
    "addon 2": {},
    "addon 3": {}
   }
  }
 },
 "payload": {
  "emptyKey": {}
 }
}
"""
  "A json object's keys" can "be counted" in {
    val main = MainSummary("")
    val json = parse(testPayload)
    
    main.countKeys(json \ "environment" \ "addons" \ "activeAddons").get should be (3)
    main.countKeys(json).get should be (2)
    main.countKeys(json \ "payload").get should be (1)
    main.countKeys(json \ "payload" \ "emptyKey").get should be (0)
    main.countKeys(json \ "dummy") should be (None)
  }
}
