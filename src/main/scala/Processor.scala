import cats.effect.IO
import domain.{AverageAccumulator, SensorAccumulatedResult, SensorMeasurement, SensorResult}

object Processor {
  def getAverage(value: AverageAccumulator): Float =
    (value.prevAvg * value.number + value.currentValue) / (value.number + 1)

  def getProcessorStream(inputStreams: Seq[fs2.Stream[IO, SensorMeasurement]]): fs2.Stream[IO, Map[String, SensorResult]] =
    inputStreams
      .fold(fs2.Stream.empty)((acc, stream) => acc ++ stream)
      .fold(Map.empty[String, SensorAccumulatedResult])((acc, elem) => {
        elem.value match {
          case Some(currentElement) =>
            val existingSensorAccumulator: Option[SensorAccumulatedResult] = acc.get(elem.sensorName)

            val newValue: SensorAccumulatedResult = existingSensorAccumulator match {
              case Some(sensorValue) =>
                val currentAverageValue: Float = Processor.getAverage(
                  AverageAccumulator(
                    sensorValue.averageAccumulator.prevAvg,
                    currentElement,
                    sensorValue.averageAccumulator.number
                  )
                )

                val newAverageAccumulator = AverageAccumulator(
                  prevAvg = currentAverageValue,
                  currentValue = sensorValue.averageAccumulator.currentValue,
                  number = sensorValue.averageAccumulator.number + 1
                )

                SensorAccumulatedResult(
                  sensorValue.min min currentElement,
                  newAverageAccumulator,
                  sensorValue.max max currentElement,
                  0,
                  0
                )
              case None =>
                val newSensorDefaultAccumulator =
                  SensorAccumulatedResult(
                    min = currentElement,
                    averageAccumulator = AverageAccumulator(0, currentElement, 1),
                    max = currentElement,
                    successCounter = 0,
                    errorCounter = 0
                  )
                newSensorDefaultAccumulator
            }

            //          println(existingSensorAccumulator)
            //          println(newValue)

            acc.updated(elem.sensorName, newValue)
          case _ => acc
        }
      })
      .evalMap(resultMap => {
        val result = resultMap.toSeq
          .map(t => {
            val (name, value) = t
            (name, SensorResult(value.min, value.averageAccumulator.prevAvg, value.max))
          })
          .sortWith((t1, t2) => t1._2.avg > t2._2.avg).toMap

        IO(result)
      })
}
