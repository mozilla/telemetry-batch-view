import awscala._
import awscala.s3._
import telemetry.streams.ExecutiveStream

object S3EventGenerator {
  implicit val s3 = S3()

  def main(args: Array[String]) {
    // TODO: dirty hack, needs to be generalized
    val inputBucket = "net-mozaws-prod-us-west-2-pipeline-data"
    val prefix = "telemetry-2"
    val date = "20151029"

    val bucket = Bucket(inputBucket)
    s3.keys(bucket, s"$prefix/$date")
      .par
      .foreach(ExecutiveStream.simulateEvent(inputBucket, _))
  }
}
