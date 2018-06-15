FROM mozilla/sbt:8u171_1.1.1

ENV _JAVA_OPTIONS="-Xms4G -Xmx4G -Xss4M -XX:MaxMetaspaceSize=512M"

WORKDIR /telemetry-batch-view
