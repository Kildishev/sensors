package processor

import cats.effect.IO
import domain.{OverallResult, SensorMeasurement}
import fs2.Stream

trait Processor {
  def getProcessorStream(inputStreams: Seq[Stream[IO, SensorMeasurement]]): Stream[IO, OverallResult]
}
