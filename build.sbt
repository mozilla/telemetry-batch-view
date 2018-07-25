javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions ++= Seq(
    "-Xmax-classfile-name", "242",
    "-feature",
    "-Ywarn-unused",
    "-Ywarn-unused-import"
)

val localMavenHttps = "https://s3-us-west-2.amazonaws.com/net-mozaws-data-us-west-2-ops-mavenrepo/"
val localMaven = "s3://net-mozaws-data-us-west-2-ops-mavenrepo/"

resolvers += "S3 local maven snapshots" at localMavenHttps + "snapshots"

// Bumped from 2.3.0 to 2.3.1 for bugfix: https://issues.apache.org/jira/browse/SPARK-23986
val sparkVersion = "2.3.1"
// Should keep hadoop version in sync with the dependency defined by Spark.
val hadoopVersion = "2.6.5"

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-batch-view",
    version := "1.1",
    scalaVersion := "2.11.8",
    // Hack to get releases to not fail to upload when the same jar name already exists. Later we will need auto versioning
    isSnapshot := true,
    scalaModuleInfo := scalaModuleInfo.value.map(_.withOverrideScalaVersion(true)),

    // Snapshot dependencies; keep in mind that changes to the master branch of these projects will get pulled in
    // automatically on next build and could cause unexpected problems while working on unrelated changes.
    libraryDependencies += "com.mozilla.telemetry" %% "moztelemetry" % "1.1-SNAPSHOT",
    libraryDependencies += "com.mozilla.telemetry" %% "spark-hyperloglog" % "2.0.0-SNAPSHOT",

    // Spark libs
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion,

    // Other dependencies
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.7.0",
    libraryDependencies += "net.sandrogrzicic" %% "scalabuff-runtime" % "1.4.0",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.7.2",
    libraryDependencies += "joda-time" % "joda-time" % "2.10",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopVersion excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.rogach" %% "scallop" % "3.1.2",
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.0",
    libraryDependencies += "org.yaml" % "snakeyaml" % "1.21",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.10.0",
    libraryDependencies += "org.scalanlp" %% "breeze" % "0.13.2",

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    libraryDependencies += "com.github.tomakehurst" % "wiremock-standalone" % "2.18.0" % Test,
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.9.0" % Test
  )

dependencyOverrides += "com.google.guava" % "guava" % "25.1-jre"
dependencyOverrides += "com.google.code.findbugs" % "jsr305" % "3.0.2"


run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
assemblyJarName in assembly := s"telemetry-batch-view-${version.value}.jar"
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

// Disable parallel execution to avoid multiple SparkContexts
parallelExecution in Test := false
logBuffered in Test := false

testOptions in Test := Seq(
  // -oD add duration reporting; see http://www.scalatest.org/user_guide/using_scalatest_with_sbt
  Tests.Argument("-oD")
)

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some("snapshots" at localMaven + "snapshots")
  else
    Some("releases"  at localMaven + "releases")
}

// Speeds up finding snapshot releases:
// https://www.scala-sbt.org/1.x/docs/Combined+Pages.html#Latest+SNAPSHOTs
updateOptions := updateOptions.value.withLatestSnapshots(false)

val scalaStyleConfigUrl = Some(url("https://raw.githubusercontent.com/mozilla/moztelemetry/master/scalastyle-config.xml"))
(scalastyleConfigUrl in Compile) := scalaStyleConfigUrl
(scalastyleConfigUrl in Test) := scalaStyleConfigUrl
