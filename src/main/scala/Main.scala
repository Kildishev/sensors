import cats.effect.{ExitCode, IO, IOApp}
import domain._
import fs2.io.file.{Files, Path}
import fs2.text
import processor.SequentialProcessor

import java.io.File
import java.nio.file.Paths
import scala.util.Try

object Main extends IOApp {
  def getStreamFromPath(filePath: Path): fs2.Stream[IO, SensorMeasurement] = {
    Files[IO].readAll(filePath)
      .through(text.utf8.decode)
      .through(text.lines)
      .map(s => {
        val tuple = s.split(",")

        SensorMeasurement(tuple(0), Try(tuple(1).toByte).toOption)
      })
  }

  def getPath(fileName: String, useResourcePath: Boolean = true): Path =
    if (useResourcePath)
      Path.fromNioPath(Paths.get(ClassLoader.getSystemResource(s"files/$fileName.csv").toURI))
    else
      Path(fileName)

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

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getPath)
    } else {
      List.empty
    }
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val folderName = args.headOption

    val paths: Seq[Path] = folderName match {
      case Some(value) =>
        getListOfFiles(value).map(filePath => getPath(filePath, useResourcePath = false))
      case None =>
        val defaultFileNames = List("leader-1", "leader-2")
        defaultFileNames.map(fileName => getPath(fileName))
    }

    val streams = paths.map(getStreamFromPath)

    for {
      _ <- SequentialProcessor.getProcessorStream(streams)
        .evalMap(m =>
          IO(println(format(m)))
        )
        .compile.drain
    } yield ExitCode.Success
  }
}
