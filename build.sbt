name := "akka-stream-throttle"

organization := "com.softwaremill"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.11"

libraryDependencies ++= {
  val akkaV = "2.5.1"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV,
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
}

parallelExecution in Test := false
parallelExecution in test := false
parallelExecution in test in Test := false
