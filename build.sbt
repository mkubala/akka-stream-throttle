name := "akka-stream-throttle"

organization := "com.softwaremill"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val akkaV = "2.5.1"
  Seq(
    "com.typesafe.akka"   %% "akka-stream"          % akkaV,
    "com.typesafe.akka"   %% "akka-stream-testkit"  % akkaV,
    "org.scalatest"       %% "scalatest"            % "3.0.1" % "test"
  )
}
