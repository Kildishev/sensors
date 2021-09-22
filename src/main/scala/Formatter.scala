import domain.OverallResult

object Formatter {
  def format(input: OverallResult): String = {
    val totalInfo =
      s"""
         |Num of processed files: ${input.streamCount}
         |Num of processed measurements: ${input.successCount + input.errorCount}
         |Num of failed measurements: ${input.errorCount}
         |
         |""".stripMargin

    val headerInfo =
      s"""
         |Sensors with highest avg humidity:
         |
         |sensor-id,min,avg,max
         |""".stripMargin

    val sensorData = input.sensorData.map(data => {
      val (name, value) = data
      value match {
        case Some(value) => s"$name,${value.min},${Math.round(value.avg)},${value.max}"
        case None => s"$name,NaN,NaN,NaN"
      }
    }).mkString("\r\n")

    totalInfo + headerInfo + sensorData
  }
}
