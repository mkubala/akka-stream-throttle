package com.softwaremill.akka.stream.throttle

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Materializer}
import com.softwaremill.akka.stream.throttle.IntervalBasedThrottlerSettings._
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

import scala.concurrent.Await
import scala.concurrent.duration._

class IntervalBasedThrottlerSpec extends FlatSpec with ParallelTestExecution with Matchers with IntervalBasedThrottlerTestKit with StreamTestingSandbox {

  val TimeTolerance: FiniteDuration = 35.millis
  val systemName = "ThrottleSpec-system"

  it should "limit rate of messages - low frequency" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](2.perSecond)
    val numberOfElements = 6

    val flow = Source(1 to numberOfElements * 2).via(throttle).runWith(TestSink.probe[Int])

    timeOfProcessing(6).elementsOf(flow) should be(3.seconds +- TimeTolerance)

    flow.cancel()
  }

  it should "limit rate of messages - medium frequency" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](100.perSecond)

    val flow = Source(1 to 400).via(throttle).runWith(TestSink.probe[Int])

    timeOfProcessing(300).elementsOf(flow) should be(3.seconds +- TimeTolerance)

    flow.cancel()
  }

  it should "limit rate of messages - high frequency" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](2000.perSecond) // max 2000 messages per second

    val flow = Source(1 to 7000).via(throttle).runWith(TestSink.probe[Int])

    timeOfProcessing(6000).elementsOf(flow) should be(3.seconds +- TimeTolerance)

    flow.cancel()
  }

  it should "keep limits with slow producer" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](1000.perSecond) // max 1000 messages per second

    val flow = Source.tick(Duration.Zero, 1.second, 0).via(throttle).runWith(TestSink.probe[Int])

    timeOfProcessing(3).elementsOf(flow, List.fill(3)(0)) should be(3.seconds +- TimeTolerance)

    flow.cancel()
  }

}

trait IntervalBasedThrottlerTestKit {

  implicit object FiniteDurationIsNumeric extends Numeric[FiniteDuration] {
    override def plus(x: FiniteDuration, y: FiniteDuration): FiniteDuration = x + y

    override def toDouble(x: FiniteDuration): Double = x.toNanos.toDouble

    override def toFloat(x: FiniteDuration): Float = x.toNanos.toFloat

    override def toInt(x: FiniteDuration): Int = x.toNanos.toInt

    override def negate(x: FiniteDuration): FiniteDuration = x.neg().toNanos.nanos

    override def fromInt(x: Int): FiniteDuration = x.nanos

    override def toLong(x: FiniteDuration): Long = x.toNanos

    override def times(x: FiniteDuration, y: FiniteDuration): FiniteDuration = x * y

    override def minus(x: FiniteDuration, y: FiniteDuration): FiniteDuration = x - y

    override def compare(x: FiniteDuration, y: FiniteDuration): Int = x compare y
  }

  protected def timeOfProcessing(n: Int) = new {

    private val initialOffset = 1
    private val defaultExpectedElements: List[Int] = (initialOffset + 1 to n + 1).inclusive.toList

    def elementsOf(flow: TestSubscriber.Probe[Int],
                   expectedElements: List[Int] = defaultExpectedElements): FiniteDuration = {
      // ask for an element, to discard stream initialization time from our measurement
      flow.request(initialOffset)
      flow.expectNext()

      val startMillis = System.currentTimeMillis()
      flow.request(n)
      flow.expectNextN(expectedElements)
      val endMillis = System.currentTimeMillis()

      (endMillis - startMillis).millis
    }
  }

}

trait StreamTestingSandbox {

  protected def systemName: String

  protected def sandbox(test: StreamTestDependencies => Unit): Unit = {
    val deps = new StreamTestDependencies(systemName)
    try {
      test(deps)
    } finally {
      deps.actorSystem.terminate()
      Await.ready(deps.actorSystem.whenTerminated, 2.seconds)
    }
  }

  protected class StreamTestDependencies(systemName: String) {
    implicit val actorSystem: ActorSystem = ActorSystem(systemName)
    implicit val materializer: Materializer = ActorMaterializer()
  }

}
