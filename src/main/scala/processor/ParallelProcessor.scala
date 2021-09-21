package processor

import cats.effect.IO
import domain.{OverallResult, SensorMeasurement}

object ParallelProcessor extends Processor {
  override def getProcessorStream(inputStreams: Seq[fs2.Stream[IO, SensorMeasurement]])
  : fs2.Stream[IO, OverallResult] = {
    // todo: Implement:)
    /**
     * Use (successCount, errorCount, Map(sensorName -> Option((min, max, totalSum, count))) to calculate it
     * for each file (in parallel), then squash all streams into one result (calculate average = totalSum / count)
     * Note: Use Long as a type for `totalSum`
     */

    ???
  }
}
