package telemetry.utils

import org.joda.time._

object Utils{
  val dict = Map("submission_url" -> "submissionURL",
                 "memory_mb" -> "memoryMB",
                 "virtual_max_mb" -> "virtualMaxMB",
                 "l2cache_kb" -> "l2cacheKB",
                 "l3cache_kb" -> "l3cacheKB",
                 "speed_mhz" -> "speedMHz",
                 "d2d_enabled" -> "D2DEnabled",
                 "d_write_enabled" -> "DWriteEnabled",
                 "vendor_id" -> "vendorID",
                 "device_id" -> "deviceID",
                 "subsys_id" -> "subsysID",
                 "ram" -> "RAM",
                 "gpu_active" -> "GPUActive",
                 "first_load_uri" -> "firstLoadURI",
                 "" -> "")

  def camelize(name: String) = {
    dict.getOrElse(name, {
                     val split = name.split("_")
                     val rest = split.drop(1).map(_.capitalize).mkString
                     split(0).mkString + rest
                   }
    )
  }

  def uncamelize(name: String) = {
    val pattern = java.util.regex.Pattern.compile("(^[^A-Z]+|[A-Z][^A-Z]+)")
    val matcher = pattern.matcher(name);
    val output = new StringBuilder

    while (matcher.find()) {
      if (output.length > 0)
        output.append("_");
      output.append(matcher.group().toLowerCase);
    }

    output.toString()
  }

  def normalizeISOTimestamp(timestamp: String) = {
    // certain date parsers, notably Presto's, have a hard time with certain date edge cases,
    // especially time zone offsets that are not between -12 and 14 hours inclusive (see bug 1250894)
    // for these time zones, we're going to use some hacky arithmetic to bring them into range;
    // they will still represent the same moment in time, just with a correct time zone
    // we're going to use the relatively lenient joda-time parser and output it in standard ISO format
    val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
    val date = dateFormatter.withOffsetParsed().parseDateTime(timestamp)
    val millisPerHour = 60 * 60 * 1000;
    val timezoneOffsetHours = date.getZone().getOffset(date).toDouble / millisPerHour
    val timezone = if (timezoneOffsetHours < -12.0) {
      org.joda.time.DateTimeZone.forOffsetMillis(((timezoneOffsetHours + 12 * Math.floor(timezoneOffsetHours / -12).toInt) * millisPerHour).toInt)
    } else if (timezoneOffsetHours > 14.0) (
      org.joda.time.DateTimeZone.forOffsetMillis(((timezoneOffsetHours - 12 * Math.floor(timezoneOffsetHours / 12).toInt) * millisPerHour).toInt)
    ) else {
      date.getZone()
    }
    dateFormatter.withZone(timezone).print(date)
  }

  def normalizeYYYYMMDDTimestamp(YYYYMMDD: String) = {
    val formatISO = org.joda.time.format.ISODateTimeFormat.dateTime()
    formatISO.withZone(org.joda.time.DateTimeZone.UTC).print(
      format.DateTimeFormat.forPattern("yyyyMMdd")
                           .withZone(org.joda.time.DateTimeZone.UTC)
                           .parseDateTime(YYYYMMDD.asInstanceOf[String])
    )
  }

  def normalizeEpochTimestamp(timestamp: BigInt) = {
    val dateFormatter = org.joda.time.format.ISODateTimeFormat.dateTime()
    val millisecondsPerDay = 1000 * 60 * 60 * 24
    dateFormatter.withZone(org.joda.time.DateTimeZone.UTC).print(new DateTime(timestamp.toLong * millisecondsPerDay))
  }
}
