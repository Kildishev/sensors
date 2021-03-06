import cats.effect.IO
import domain.{OverallResult, SensorMeasurement, SensorResult}
import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import processor.{ParallelProcessor, Processor, SequentialProcessor}
import fs2.Stream

class ProcessorSpec extends AnyFlatSpec {
  val processors: Seq[Processor] = List(SequentialProcessor, ParallelProcessor)

  "The result of a list of streams" should "be calculated" in {
    processors.foreach(processor => {
      val s1 = "s1"
      val s2 = "s2"
      val s3 = "s3"

      val inputStream = Stream[IO, SensorMeasurement](
        SensorMeasurement(s1, Some(0)),
        SensorMeasurement(s1, None),
        SensorMeasurement(s1, Some(5)),
      )

      val inputStream2 = Stream[IO, SensorMeasurement](
        SensorMeasurement(s2, Some(10)),
        SensorMeasurement(s1, Some(10)),
        SensorMeasurement(s2, Some(10)),
      )

      val inputStream3 = Stream[IO, SensorMeasurement](
        SensorMeasurement(s3, None),
        SensorMeasurement(s3, None),
        SensorMeasurement(s2, Some(100)),
      )

      val resultIO = for {
        result <- processor.getProcessorStream(Seq(inputStream, inputStream2, inputStream3)).compile.last
      } yield result

      val expectedOverallResult = OverallResult(
        successCount = 6,
        errorCount = 3,
        streamCount = 3,
        sensorData = Map(
          s2 -> SensorResult(10, 40.0, 100).some,
          s1 -> SensorResult(0, 5.0, 10).some,
          s3 -> None,
        )
      ).some

      assertResult(expectedOverallResult)(resultIO.unsafeRunSync())
    })
  }

  "The results for simple streams with the same elements but different order" should "be the same" in {
    processors.foreach(processor => {
      val s1 = "s1"
      val s2 = "s2"

      val inputList = List(
        SensorMeasurement(s1, Some(0)),
        SensorMeasurement(s1, Some(10)),
        SensorMeasurement(s1, Some(5)),

        SensorMeasurement(s2, Some(0)),
        SensorMeasurement(s2, Some(10)),
        SensorMeasurement(s2, Some(5)),
      )
      val inputStream = Stream.emits[IO, SensorMeasurement](inputList)
      val inputStream2 = Stream.emits[IO, SensorMeasurement](inputList.sortBy(_.value))

      val resultIO = for {
        result <- processor.getProcessorStream(Seq(inputStream)).compile.last
        result2 <- processor.getProcessorStream(Seq(inputStream2)).compile.last
      } yield result == result2

      assert(resultIO.unsafeRunSync())
    })
  }
}
