import cats.effect.{IO, IOApp}
import domain._
import fs2.io.file.{Files, Path}
import fs2.text
import processor.SequentialProcessor

import java.nio.file.Paths
import scala.util.Try

object Main extends IOApp.Simple {
  def streamFromFile(fileName: String): fs2.Stream[IO, SensorMeasurement] =
    Files[IO].readAll(Path.fromNioPath(Paths.get(ClassLoader.getSystemResource(s"files/$fileName.csv").toURI)))
      .through(text.utf8.decode)
      .through(text.lines)
      .map(s => {
        val tuple = s.split(",")

        SensorMeasurement(tuple(0), Try(tuple(1).toByte).toOption)
      })

  def format(input: OverallResult): String = {
    val totalInfo =
      s"""
         |Num of processed files: ${input.streamCount}
         |Num of processed measurements: ${input.successCount + input.errorCount}
         |Num of failed measurements: ${input.errorCount}
         |
         |""".stripMargin
    val headerInfo =
      s"""
         |Sensors with highest avg humidity:
         |
         |sensor-id,min,avg,max
         |""".stripMargin
    val sensorData = input.sensorData.map(data => {
      val (name, value) = data
      value match {
        case Some(value) => s"$name,${value.min},${Math.round(value.avg)},${value.max}"
        case None => s"$name,NaN,NaN,NaN"
      }
    }).mkString("\r\n")

    totalInfo + headerInfo + sensorData
  }

  override def run: IO[Unit] = {
    val fileNameList = List("leader-1", "leader-2")
    val streams = fileNameList.map(streamFromFile)

    for {
      _ <- IO.println("Start processing...")
      _ <- SequentialProcessor.getProcessorStream(streams)
        .evalMap(m =>
          IO(println(format(m)))
        )
        .compile.drain
      _ <- IO.println("Done!")
    } yield ()
  }
}
