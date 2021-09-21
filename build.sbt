name := "sensors1"

version := "0.1"

scalaVersion := "2.13.6"

val fs2Version = "3.1.1"

// available for 2.12, 2.13, 3.0
libraryDependencies += "co.fs2" %% "fs2-core" % fs2Version

// optional I/O library
libraryDependencies += "co.fs2" %% "fs2-io" % fs2Version

// optional reactive streams interop
libraryDependencies += "co.fs2" %% "fs2-reactive-streams" % fs2Version


val scalaTestVersion = "3.2.9"
libraryDependencies += "org.scalactic" %% "scalactic" % scalaTestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"