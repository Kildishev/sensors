package domain

import cats.effect.IO

trait Processor {
  def getProcessorStream(inputStreams: Seq[fs2.Stream[IO, SensorMeasurement]]): fs2.Stream[IO, OverallResult]
}
