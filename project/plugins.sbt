resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")