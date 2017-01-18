organization := "net.hamnaberg"

name := "der-process"

crossScalaVersions := List("2.12.1","2.11.8")

scalaVersion := crossScalaVersions.value.head

libraryDependencies += "co.fs2" %% "fs2-core" % "0.9.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.1"
