javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val localMavenHttps = "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-mavenrepo/"
val localMaven = "s3://net-mozaws-data-us-west-2-ops-mavenrepo/"

resolvers += "S3 local maven snapshots" at localMavenHttps + "snapshots"

val sparkVersion = "2.0.2"

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-batch-view",
    version := "1.1",
    scalaVersion := "2.11.8",
    retrieveManaged := true,
    // Hack to get releases to not fail to upload when the same jar name already exists. Later we will need auto versioning
    isSnapshot := true,
    scalaModuleInfo := scalaModuleInfo.value.map(_.withOverrideScalaVersion(true)),
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.1-SNAPSHOT",
    libraryDependencies += "com.mozilla.telemetry" %% "spark-hyperloglog" % "2.0.0-SNAPSHOT",
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.7.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4",
    libraryDependencies += "net.sandrogrzicic" %% "scalabuff-runtime" % "1.4.0",
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.7" % "test",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.2",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.1" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.10.0",
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion,
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies += "org.yaml" % "snakeyaml" % "1.17",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "com.github.tomakehurst" % "wiremock-standalone" % "2.8.0"
  )

/*
 The HBase client requires protobuf-java 2.5.0 but scalapb uses protobuf-java 3.x
 so we have to force the dependency here. This should be fine as we are using only
 version 2 of the protobuf spec.
*/
dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "2.5.0"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
assemblyJarName in assembly := s"telemetry-batch-view-${version.value}.jar"
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Disable parallel execution to avoid multiple SparkContexts
parallelExecution in Test := false
logBuffered in Test := false

lazy val Slow = config("slow").extend(Test)
configs(Slow)
inConfig(Slow)(Defaults.testTasks)

testOptions in Test := Seq(
  Tests.Argument("-l", "org.scalatest.tags.Slow"),
  // -oD add duration reporting; see http://www.scalatest.org/user_guide/using_scalatest_with_sbt
  Tests.Argument("-oD"))
testOptions in Slow := Seq(
  // -oD add duration reporting; see http://www.scalatest.org/user_guide/using_scalatest_with_sbt
  Tests.Argument("-oD"))

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at localMaven + "snapshots")
  else
    Some("releases"  at localMaven + "releases")
}
