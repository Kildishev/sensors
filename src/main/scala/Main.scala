import FileHelper._
import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.Path
import processor.{Processor, SequentialProcessor}

object Main extends IOApp {

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
    val processor: Processor = SequentialProcessor

    for {
      _ <- processor.getProcessorStream(streams)
        .evalMap(m =>
          IO(println(Formatter.format(m)))
        )
        .compile.drain
    } yield ExitCode.Success
  }
}
