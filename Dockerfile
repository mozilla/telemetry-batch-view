FROM hseeberger/scala-sbt:8u151-2.12.4-1.1.1

ENV _JAVA_OPTIONS="-Xms4G -Xmx4G -Xss4M -XX:MaxMetaspaceSize=512M"

WORKDIR /telemetry-batch-view
