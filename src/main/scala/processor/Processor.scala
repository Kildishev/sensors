package processor

import cats.effect.IO
import domain.{OverallResult, SensorMeasurement}

trait Processor {
  def getProcessorStream(inputStreams: Seq[fs2.Stream[IO, SensorMeasurement]]): fs2.Stream[IO, OverallResult]
}
