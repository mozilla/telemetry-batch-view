package telemetry.utils

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
}
