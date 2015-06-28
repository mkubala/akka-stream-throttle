package com.softwaremill.akka.stream.throttle

import java.util.concurrent.TimeUnit

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ThrottleSettingsSpec extends FlatSpec with Matchers {

  it should "return correct minimum interval between events" in {
    assertInterval(ThrottleSettings(2, TimeUnit.SECONDS), 500.millis)
    assertInterval(ThrottleSettings(20, TimeUnit.SECONDS), 50.millis)
    assertInterval(ThrottleSettings(200, TimeUnit.SECONDS), 5.millis)
    assertInterval(ThrottleSettings(120, TimeUnit.MINUTES), 500.millis)
    assertInterval(ThrottleSettings(1000, TimeUnit.SECONDS), 1.millis)
  }

  private def assertInterval(ts: ThrottleSettings, expectedInterval: FiniteDuration): Unit = {
    ts.interval.compare(expectedInterval) shouldBe 0
  }

}
