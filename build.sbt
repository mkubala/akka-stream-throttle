name := "akka-stream-throttle"

organization := "com.softwaremill"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC4",
  "com.typesafe.akka" %% "akka-stream-testkit-experimental" % "1.0-RC4",
  "org.scalatest" %% "scalatest" % "2.2.4"
)
