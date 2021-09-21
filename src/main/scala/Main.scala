import cats.effect.{IO, IOApp}
import domain._
import fs2.io.file.{Files, Path}
import fs2.text

import scala.util.Try

object Main extends IOApp.Simple {
  def streamFromFile(fileName: String): fs2.Stream[IO, SensorMeasurement] =
    Files[IO].readAll(Path(s"/home/xatm092/work/github/sensors1/src/main/scala/files/$fileName.csv"))
      .through(text.utf8.decode)
      .through(text.lines)
      .map(s => {
        val tuple = s.split(",")

        SensorMeasurement(tuple(0), Try(tuple(1).toByte).toOption)
      })

  override def run: IO[Unit] = {
    val fileNameList = List("leader-1", "leader-2")
    val streams = fileNameList.map(streamFromFile)

    for {
      _ <- IO.println("Start processing...")
      _ <- Processor.getProcessorStream(streams)
        .evalMap(m => IO(println(m))).compile.drain
      _ <- IO.println("Done!")
    } yield ()
  }
}
