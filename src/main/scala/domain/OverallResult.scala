package domain

case class OverallResult(
                          successCount: Int,
                          errorCount: Int,
                          streamCount: Int,
                          sensorData: Map[String, Option[SensorResult]]
                        )
