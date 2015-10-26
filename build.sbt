// name := "telemetry-parquet-converter"
// version := "1.0"
// scalaVersion := "2.11.6"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-parquet-converter",
    version := "1.0",
    scalaVersion := "2.11.7",
    retrieveManaged := true,
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "com.twitter" % "parquet-avro" % "1.6.0",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.1",
    libraryDependencies += "com.github.seratch" %% "awscala" % "0.5.+",
    libraryDependencies += "net.sandrogrzicic" %% "scalabuff-runtime" % "1.4.0",
    libraryDependencies += "com.typesafe" % "config" % "1.3.0",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
    libraryDependencies += "com.amazonaws" % "aws-lambda-java-core" % "1.0.0",
    libraryDependencies += "com.amazonaws" % "aws-lambda-java-events" % "1.0.0"
  )

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
