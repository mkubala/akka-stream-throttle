package com.softwaremill.akka.stream.throttle

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class ThrottleSpec extends FlatSpec with Matchers {

  val ToleranceInMillis = 35

  it should "limit rate of messages - low frequency" in {
    implicit val actorSystem = ActorSystem("ThrottleSpec-system")
    implicit val materializer = ActorMaterializer()

    val throttle = Throttle.create[Int](ThrottleSettings(2, TimeUnit.SECONDS)) // max 2 messages per second
    val numberOfElements = 6

    val flow = Source(1 to numberOfElements * 2).via(throttle).runWith(TestSink.probe[Int])

    flow.request(1)
    flow.expectNext()
    val startMillis = System.currentTimeMillis()
    flow.request(numberOfElements).expectNextN(2 to numberOfElements + 1)
    val endMillis = System.currentTimeMillis()

    (endMillis - startMillis) should be >= 3.seconds.toMillis - ToleranceInMillis
    (endMillis - startMillis) should be < 3.seconds.toMillis + ToleranceInMillis
    flow.cancel()
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }

  it should "limit rate of messages - medium frequency" in {
    implicit val actorSystem = ActorSystem("ThrottleSpec-system")
    implicit val materializer = ActorMaterializer()

    val throttle = Throttle.create[Int](ThrottleSettings(100, TimeUnit.SECONDS)) // max 100 messages per second
    val numberOfElements = 300

    val flow = Source(1 to numberOfElements * 2).via(throttle).runWith(TestSink.probe[Int])

    flow.request(1)
    flow.expectNext()
    val startMillis = System.currentTimeMillis()
    flow.request(numberOfElements).expectNextN(2 to numberOfElements + 1)
    val endMillis = System.currentTimeMillis()

    (endMillis - startMillis) should be >= 3.seconds.toMillis - ToleranceInMillis
    (endMillis - startMillis) should be < 3.seconds.toMillis + ToleranceInMillis
    flow.cancel()
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }

  it should "limit rate of messages - high frequency" in {
    implicit val actorSystem = ActorSystem("ThrottleSpec-system")
    implicit val materializer = ActorMaterializer()

    val throttle = Throttle.create[Int](ThrottleSettings(2000, TimeUnit.SECONDS)) // max 2000 messages per second

    val flow = Source(1 to 7000).via(throttle).runWith(TestSink.probe[Int])

    flow.request(1)
    flow.expectNext()
    val startMillis = System.currentTimeMillis()
    flow.request(6000).expectNextN(2 to 6001)
    val endMillis = System.currentTimeMillis()

    (endMillis - startMillis) should be >= 3.seconds.toMillis - ToleranceInMillis
    flow.cancel()
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }

  it should "keep limits with slow producer" in {
    implicit val actorSystem = ActorSystem("ThrottleSpec-system")
    implicit val materializer = ActorMaterializer()

    val throttle = Throttle.create[Int](ThrottleSettings(1000, TimeUnit.SECONDS)) // max 1000 messages per second

    val flow = Source(Duration.Zero, 1.second, 0).via(throttle).runWith(TestSink.probe[Int])

    flow.request(1)
    flow.expectNext()
    val startMillis = System.currentTimeMillis()
    flow.request(3)
    flow.expectNextN(List.fill(3)(0))
    val endMillis = System.currentTimeMillis()

    (endMillis - startMillis) should be >= 3.seconds.toMillis - ToleranceInMillis
    (endMillis - startMillis) should be < 3.seconds.toMillis + ToleranceInMillis
    flow.cancel()
    actorSystem.shutdown()
    actorSystem.awaitTermination(10.seconds)
  }

}
