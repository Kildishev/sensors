package processor

import cats.effect.IO
import cats.implicits.catsSyntaxOptionId
import domain._

object SequentialProcessor extends Processor {

  case class AccumulatedResult(
                                sensorData: SensorDataAccumulated,
                                successCount: Int,
                                errorCount: Int
                              )

  override def getProcessorStream(
                                   inputStreams: Seq[fs2.Stream[IO, SensorMeasurement]]
                                 ): fs2.Stream[IO, OverallResult] =
    inputStreams
      .fold(fs2.Stream.empty)((acc, stream) => acc ++ stream)
      .fold(AccumulatedResult(Map.empty, 0, 0))((acc, elem) => {
        val sensorDataAccumulated = acc.sensorData
        val existingSensorAccumulator: Option[SensorAccumulatedResult] =
          sensorDataAccumulated.get(elem.sensorName).flatten

        val sensorName = elem.sensorName
        val optionalParsedValue = elem.value

        optionalParsedValue match {
          case Some(parsedValue) =>
            AccumulatedResult(
              sensorData = getNewAccumulatorForAValidValue(
                sensorName = sensorName,
                validValue = parsedValue,
                existingSensorAccumulator = existingSensorAccumulator,
                sensorDataAccumulated = sensorDataAccumulated
              ),
              successCount = acc.successCount + 1,
              errorCount = acc.errorCount
            )

          case _ =>
            getAccumulatorForInvalidValue(sensorName, existingSensorAccumulator, sensorDataAccumulated, acc)
        }
      })
      .evalMap(accumulatedResult => {
        val resultSensorData = accumulatedResult.sensorData.toSeq
          .map(tuple => {
            val (sensorName, sensorAccumulatorValue) = tuple
            val sensorResult = sensorAccumulatorValue.map(value =>
              SensorResult(
                min = value.min,
                avg = value.averageAccumulator.prevAvg,
                max = value.max)
            )
            sensorName -> sensorResult
          })
          .sortWith((t1, t2) => {
            val leftAverage = t1._2.map(_.avg).getOrElse(-1.0)
            val rightAverage = t2._2.map(_.avg).getOrElse(-1.0)
            leftAverage > rightAverage
          })
          .toMap

        val result = OverallResult(
          successCount = accumulatedResult.successCount,
          errorCount = accumulatedResult.errorCount,
          streamCount = inputStreams.length,
          sensorData = resultSensorData
        )

        IO(result)
      })

  private def getNewAccumulatorForAValidValue(
                                               sensorName: String,
                                               validValue: Byte,
                                               existingSensorAccumulator: Option[SensorAccumulatedResult],
                                               sensorDataAccumulated: SensorDataAccumulated
                                             ): Map[String, Option[SensorAccumulatedResult]] = {
    val newValue: Option[SensorAccumulatedResult] = existingSensorAccumulator match {
      case Some(sensorValue) =>
        val currentNumber = sensorValue.averageAccumulator.number

        val currentAverageValue: Float = SequentialProcessor.getAverage(
          AverageAccumulator(
            sensorValue.averageAccumulator.prevAvg,
            validValue,
            currentNumber
          )
        )

        val newAverageAccumulator = AverageAccumulator(
          prevAvg = currentAverageValue,
          currentValue = sensorValue.averageAccumulator.currentValue,
          number = currentNumber + 1
        )

        SensorAccumulatedResult(
          min = sensorValue.min min validValue,
          averageAccumulator = newAverageAccumulator,
          max = sensorValue.max max validValue,
          successCounter = sensorValue.successCounter + 1,
          errorCounter = sensorValue.errorCounter
        ).some
      case None =>
        val newSensorDefaultAccumulator =
          SensorAccumulatedResult(
            min = validValue,
            averageAccumulator = AverageAccumulator(validValue, validValue, 1),
            max = validValue,
            successCounter = 1,
            errorCounter = 0
          )
        newSensorDefaultAccumulator.some
    }

    sensorDataAccumulated.updated(sensorName, newValue)
  }

  private def getAccumulatorForInvalidValue(
                                             sensorName: String,
                                             existingSensorAccumulator: Option[SensorAccumulatedResult],
                                             sensorDataAccumulated: SensorDataAccumulated,
                                             acc: AccumulatedResult
                                           ): AccumulatedResult = {
    val sensorData = existingSensorAccumulator match {
      case Some(value) =>
        sensorDataAccumulated.updated(sensorName, value.copy(errorCounter = value.errorCounter + 1).some)
      case _ =>
        sensorDataAccumulated.updated(sensorName, None)
    }

    AccumulatedResult(
      sensorData = sensorData, successCount = acc.successCount, errorCount = acc.errorCount + 1
    )
  }

  private def getAverage(value: AverageAccumulator): Float =
    (value.prevAvg * value.number + value.currentValue) / (value.number + 1)
}
