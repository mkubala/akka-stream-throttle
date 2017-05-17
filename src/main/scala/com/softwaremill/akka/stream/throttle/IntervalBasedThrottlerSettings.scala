package com.softwaremill.akka.stream.throttle

import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

case class IntervalBasedThrottlerSettings(numberOfOps: Int, duration: FiniteDuration) {
  lazy val interval: FiniteDuration = {
    val oneUnitInNanos: Long = duration.toNanos
    val intervalInNanos: BigDecimal = oneUnitInNanos / numberOfOps
    intervalInNanos.setScale(0, RoundingMode.UP).toLong.nanos
  }
}

object IntervalBasedThrottlerSettings {

  implicit class ThrottleSettingsBuilder(val numberOfOps: Int) extends AnyVal {
    def per(duration: FiniteDuration): IntervalBasedThrottlerSettings = IntervalBasedThrottlerSettings(numberOfOps, duration)
    def perSecond: IntervalBasedThrottlerSettings = per(1.second)
  }

}
