javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven/"

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-batch-view",
    version := "1.1",
    scalaVersion := "2.11.8",
    retrieveManaged := true,
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf",
    libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0",
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.7.0",
    libraryDependencies += "com.github.seratch" %% "awscala" % "0.3.+",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.2",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.1" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.10.0",
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0",
    libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.3",
    libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.3",
    libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.3",
    libraryDependencies += "eu.unicredit" %% "hbase-rdd" % "0.7.1",
    libraryDependencies += "vitillo" % "spark-hyperloglog" % "1.1.1",
    libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0",
    libraryDependencies +=  "org.yaml" % "snakeyaml" % "1.17"
  )

/*
  The HBase client requires protobuf-java 2.5.0 but scalapb uses protobuf-java 3.x
  so we have to force the dependency here. This should be fine as we are using only
  version 2 of the protobuf spec.
*/
dependencyOverrides += "com.google.protobuf" % "protobuf-java" % "2.5.0"

// Compile proto files
PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

// Exclude generated classes from the coverage
coverageExcludedPackages := "com\\.mozilla\\.telemetry\\.heka\\.(Field|Message|Header)"

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
assemblyJarName in assembly := s"telemetry-batch-view-${version.value}.jar"
test in assembly := {}

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x => MergeStrategy.first
}

// Disable parallel execution to avoid multiple SparkContexts
parallelExecution in Test := false

lazy val Slow = config("slow").extend(Test)
configs(Slow)
inConfig(Slow)(Defaults.testTasks)

testOptions in Test := Seq(Tests.Argument("-l", "org.scalatest.tags.Slow"))
testOptions in Slow := Seq()
