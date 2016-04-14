javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

sparkVersion := "1.6.0"

sparkComponents ++= Seq("yarn", "sql", "hive")

spDependencies += "vitillo/spark-hyperloglog:1.0.2"

lazy val root = (project in file(".")).
  settings(
    name := "telemetry-batch-view",
    version := "1.1",
    scalaVersion := "2.10.6",
    retrieveManaged := true,
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    libraryDependencies += "org.apache.avro" % "avro" % "1.7.7",
    libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1",
    libraryDependencies += "com.github.seratch" %% "awscala" % "0.3.+",
    libraryDependencies += "net.sandrogrzicic" %% "scalabuff-runtime" % "1.4.0",
    libraryDependencies += "com.typesafe" % "config" % "1.2.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4",
    libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.10",
    libraryDependencies += "joda-time" % "joda-time" % "2.9.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4" excludeAll(ExclusionRule(organization = "javax.servlet")),
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.10.0",
    libraryDependencies += "org.rogach" %% "scallop" % "1.0.0"
  )

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
assemblyJarName in assembly := s"telemetry-batch-view-${version.value}.jar"
test in assembly := {}

mergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x => MergeStrategy.first
}
