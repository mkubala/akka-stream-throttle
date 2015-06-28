package com.softwaremill.akka.stream.throttle

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

case class ThrottleSettings(numberOfOps: Int, timeUnit: TimeUnit) {
  lazy val interval: FiniteDuration = {
    val oneUnitInNanos: Long = timeUnit.toNanos(1)
    val intervalInNanos = oneUnitInNanos / numberOfOps
    intervalInNanos.nanos
  }
}
