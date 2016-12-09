package com.softwaremill.akka.stream.throttle

import scala.concurrent.duration._

case class ThrottleSettings(numberOfOps: Int, duration: FiniteDuration) {
  lazy val interval: FiniteDuration = {
    val oneUnitInNanos: Long = duration.toNanos
    val intervalInNanos = oneUnitInNanos / numberOfOps
    intervalInNanos.nanos
  }
}

object ThrottleSettings {

  implicit class ThrottleSettingsBuilder(val numberOfOps: Int) extends AnyVal {
    def per(duration: FiniteDuration): ThrottleSettings = ThrottleSettings(numberOfOps, duration)
    def perSecond: ThrottleSettings = per(1.second)
  }

}
