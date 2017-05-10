package com.softwaremill.akka.stream.throttle

import scala.concurrent.duration._

case class IntervalBasedThrottlerSettings(numberOfOps: Int, duration: FiniteDuration) {
  lazy val interval: FiniteDuration = {
    val oneUnitInNanos: Long = duration.toNanos
    val intervalInNanos = oneUnitInNanos / numberOfOps
    intervalInNanos.nanos
  }
}

object IntervalBasedThrottlerSettings {

  implicit class ThrottleSettingsBuilder(val numberOfOps: Int) extends AnyVal {
    def per(duration: FiniteDuration): IntervalBasedThrottlerSettings = IntervalBasedThrottlerSettings(numberOfOps, duration)
    def perSecond: IntervalBasedThrottlerSettings = per(1.second)
  }

}
