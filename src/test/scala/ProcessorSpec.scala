import cats.effect.IO
import domain.{SensorMeasurement, SensorResult}
import org.scalatest.flatspec.AnyFlatSpec
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId

class ProcessorSpec extends AnyFlatSpec {
  "THe result of a simple stream" should "be calculated" in {
    val s1 = "s1"
    val s2 = "s2"

    val inputStream = fs2.Stream[IO, SensorMeasurement](
      SensorMeasurement(s1, Some(0)),
      SensorMeasurement(s1, Some(10)),
      SensorMeasurement(s1, Some(5)),

      SensorMeasurement(s2, Some(0)),
      SensorMeasurement(s2, Some(10)),
      SensorMeasurement(s2, Some(5)),
    )

    val resultIO = for {
      result <- Processor.getProcessorStream(Seq(inputStream)).compile.last
    } yield result

    assertResult(Map(s1 -> SensorResult(0, 5.0, 10), s2 -> SensorResult(0, 5.0, 10)).some)(resultIO.unsafeRunSync())
  }

  "The results of simple streams with the same elements but different order" should "be the same" in {
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
    val inputStream = fs2.Stream.emits[IO, SensorMeasurement](inputList)
    val inputStream2 = fs2.Stream.emits[IO, SensorMeasurement](inputList.sortBy(_.value))

    val resultIO = for {
      result <- Processor.getProcessorStream(Seq(inputStream)).compile.last
      result2 <- Processor.getProcessorStream(Seq(inputStream2)).compile.last
    } yield result == result2

    assert(resultIO.unsafeRunSync())
  }
}
