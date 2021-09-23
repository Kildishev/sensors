import cats.effect.IO
import domain.SensorMeasurement
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.Stream

import java.io.File
import java.nio.file.Paths
import scala.util.Try

object FileHelper {
  def getStreamFromPath(filePath: Path): Stream[IO, SensorMeasurement] = {
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

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getPath)
    } else {
      List.empty
    }
  }

}
