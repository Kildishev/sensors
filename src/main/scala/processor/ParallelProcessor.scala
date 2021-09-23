package processor

import cats.effect.IO
import cats.implicits.{catsSyntaxOptionId, catsSyntaxSemigroup}
import cats.kernel.{Monoid, Semigroup}
import domain.{OverallResult, SensorMeasurement, SensorResult}
import fs2.Stream

object ParallelProcessor extends Processor {
  override def getProcessorStream(inputStreams: Seq[Stream[IO, SensorMeasurement]])
  : Stream[IO, OverallResult] = {
    val streams = Stream.emits(inputStreams.map(linearProcessor))
    val result: Stream[IO, SteamOverallData] = squashStreams(streams, 4)

    result
      .through(s => calculateResult(s, inputStreams.length))
      .map(overallResult => overallResult.copy(sensorData = overallResult.sensorData
        .toSeq
        .sortWith((t1, t2) => {
          val leftAverage = t1._2.map(_.avg).getOrElse(-1.0)
          val rightAverage = t2._2.map(_.avg).getOrElse(-1.0)

          leftAverage > rightAverage
        })
        .toMap
      ))
  }

  type SensorName = String

  case class SensorSummaryData(
                                min: Byte,
                                max: Byte,
                                totalSum: Long,
                                count: Int
                              )

  type SensorSummaryDataMap = Map[SensorName, Option[SensorSummaryData]]

  case class SteamOverallData(
                               successCount: Int,
                               errorCount: Int,
                               sensorData: SensorSummaryDataMap
                             )

  private val defaultOverallData = SteamOverallData(0, 0, Map.empty)

  private def linearProcessor(inputStream: Stream[IO, SensorMeasurement]): Stream[IO, SteamOverallData] = {
    inputStream
      .fold(defaultOverallData)((acc, elem) => {
        val sensorName = elem.sensorName
        val currentSensorData = acc.sensorData
        val existingSensorData: Option[SensorSummaryData] = currentSensorData.get(elem.sensorName).flatten

        val currentElementValue = elem.value

        val newAccumulator: SteamOverallData = currentElementValue match {
          case Some(currentElementValue) =>
            val updatedSensorDataForAValidValue: SensorSummaryData = existingSensorData match {
              case Some(value) =>
                SensorSummaryData(
                  min = value.min min currentElementValue,
                  max = value.max max currentElementValue,
                  totalSum = value.totalSum + currentElementValue,
                  count = value.count + 1
                )
              case None =>
                SensorSummaryData(
                  min = currentElementValue,
                  max = currentElementValue,
                  totalSum = currentElementValue,
                  count = 1
                )
            }

            acc.copy(
              successCount = acc.successCount + 1,
              sensorData = acc.sensorData.updated(sensorName, updatedSensorDataForAValidValue.some)
            )
          case None =>
            val increasedErrorCount = acc.errorCount + 1

            existingSensorData match {
              case None =>
                val nanSensorData = acc.sensorData.updated(sensorName, None)
                acc.copy(
                  errorCount = increasedErrorCount,
                  sensorData = nanSensorData
                )
              case Some(_) => acc.copy(errorCount = increasedErrorCount)
            }
        }

        newAccumulator
      })
  }

  private def squashStreams(
                             streams: Stream[IO, Stream[IO, SteamOverallData]],
                             parNumber: Int,
                           ): Stream[IO, SteamOverallData] = {
    streams.parJoin(parNumber).fold(defaultOverallData)((acc, elem) => {
      val squashedMaps: SensorSummaryDataMap = squashTwoResults(acc.sensorData, elem.sensorData)

      SteamOverallData(
        successCount = acc.successCount + elem.successCount,
        errorCount = acc.errorCount + elem.errorCount,
        sensorData = squashedMaps
      )
    })
  }

  private def calculateResult(
                               input: Stream[IO, SteamOverallData],
                               streamCount: Int,
                             ): Stream[IO, OverallResult] = {
    val result = input.compile.last
      .map(_.map(data => {
        val sensorData: Map[SensorName, Option[SensorResult]] = data.sensorData.map(tuple => {
          val (sensorName, sensorValue) = tuple

          sensorName -> sensorValue.map(value => {
            val count = if (value.count > 0) value.count else 1
            SensorResult(
              value.min,
              value.totalSum / count,
              value.max
            )
          })
        })

        OverallResult(
          successCount = data.successCount,
          errorCount = data.errorCount,
          streamCount = streamCount,
          sensorData = sensorData
        )
      }).getOrElse(OverallResult(0, 0, 0, Map.empty)))

    Stream.eval(result)
  }

  implicit val sensorSummaryDataSemigroup: Semigroup[SensorSummaryData] =
    (x: SensorSummaryData, y: SensorSummaryData) => SensorSummaryData(
      min = x.min min y.min,
      max = x.max max y.max,
      totalSum = x.totalSum + y.totalSum,
      count = x.count + y.count,
    )

  implicit val sensorSummaryDataMonoid: Monoid[Option[SensorSummaryData]] = new Monoid[Option[SensorSummaryData]] {
    override def empty: Option[SensorSummaryData] = None

    override def combine(x: Option[SensorSummaryData], y: Option[SensorSummaryData]): Option[SensorSummaryData] =
      (x, y) match {
        case (Some(x), Some(y)) => (x |+| y).some
        case (Some(x), None) => x.some
        case (None, Some(y)) => y.some
        case _ => None
      }
  }

  private def squashTwoResults(l: SensorSummaryDataMap, r: SensorSummaryDataMap): SensorSummaryDataMap = {
    val merged: Seq[(SensorName, Option[SensorSummaryData])] = l.toSeq ++ r.toSeq
    val groupedBySensorName: Map[SensorName, Seq[(SensorName, Option[SensorSummaryData])]] = merged.groupBy(_._1)

    val result: Map[SensorName, Option[SensorSummaryData]] = groupedBySensorName.map(tuple => {
      val sensorName = tuple._1
      val sensorData: List[Option[SensorSummaryData]] = tuple._2.map(_._2).toList

      sensorName -> Monoid[Option[SensorSummaryData]].combineAll(sensorData)
    })

    result
  }
}
