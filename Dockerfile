FROM java:8

ENV SBT_VERSION 0.13.13
ENV HBASE_VERSION 1.2.3

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  sbt sbtVersion

# Install hbase
RUN wget -nv https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz
RUN tar -zxf hbase-$HBASE_VERSION-bin.tar.gz
RUN rm hbase-$HBASE_VERSION-bin.tar.gz

ENV _JAVA_OPTIONS="-Xms4G -Xmx4G -Xss4M -XX:MaxMetaspaceSize=256M"

WORKDIR /telemetry-batch-view
