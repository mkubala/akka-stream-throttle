package com.softwaremill.akka.stream.throttle

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.OnNext
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, Materializer, ThrottleMode}
import com.softwaremill.akka.stream.throttle.IntervalBasedThrottlerSettings._
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class IntervalBasedThrottlerSpec extends FlatSpec /*with ParallelTestExecution*/ with Matchers
  with IntervalBasedThrottlerTestKit with StreamTestingSandbox {

  val TimeTolerance: FiniteDuration = 50.millis

  val systemName = "ThrottleSpec-system"

  it should "limit rate of messages - low frequency" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](2.perSecond)

    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])

    val (time, avgInterval) = timeOfProcessing(6, 1).elementsOf(flow)
    time should be >= 2.5.seconds
    time should be <= 3.seconds

    flow.cancel()
  }

  //  it should "limit rate of messages - medium frequency" in sandbox { deps =>
  //    import deps._
  //
  //    val throttle = IntervalBasedThrottler.create[Int](100.perSecond)
  //
  //    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])
  //
  //    val (time, avgInterval) = timeOfProcessing(300, 1).elementsOf(flow)
  //    time should be >= 3.seconds
  //    time should be <= 3.5.seconds
  //
  //    flow.cancel()
  //  }
  //
  //  it should "limit rate of messages - moderate frequency" in sandbox { deps =>
  //    import deps._
  //
  //    val throttle = IntervalBasedThrottler.create[Int](200.perSecond) // max 2000 messages per second
  //
  //    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])
  //
  //    val (time, avgInterval) = timeOfProcessing(600, 2).elementsOf(flow)
  //    time should be >= 3.seconds
  //    time should be <= 3.5.seconds
  //
  //    flow.cancel()
  //  }
  //
  //  it should "limit rate of messages - high frequency" in sandbox { deps =>
  //    import deps._
  //
  //    val throttle = IntervalBasedThrottler.create[Int](2000.perSecond) // max 2000 messages per second
  //
  //    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])
  //
  //    val (time, avgInterval) = timeOfProcessing(6000, 20).elementsOf(flow)
  //    time should be >= 3.seconds
  //    time should be <= 3.5.seconds
  //
  //    flow.cancel()
  //  }
  //
  //  it should "limit rate of messages - extremely high frequency" in sandbox { deps =>
  //    import deps._
  //
  //    val throttle = IntervalBasedThrottler.create[Int](50000.perSecond) // max 2000 messages per second
  //
  //    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])
  //
  //    val (time, avgInterval) = timeOfProcessing(150000, 500).elementsOf(flow)
  //    time should be >= 3.seconds
  //    time should be <= 3.5.seconds
  //
  //    flow.cancel()
  //  }

  // ----

    it should "limit rate of messages - medium frequency" in sandbox { deps =>
      import deps._

      val throttle = IntervalBasedThrottler.create[Int](100.perSecond)

      val flow = infiniteSourceWithRandomDelay(100.millis).via(throttle).runWith(TestSink.probe[Seq[Int]])

      val (_, intervals) = timeOfProcessing(300, 1).elementsOf(flow)
      val minInterval = intervals.min
      minInterval should be >= 3.seconds
      minInterval should be <= 3.5.seconds

      flow.cancel()
    }

  // ----

  it should "keep limits with slow producer" in sandbox { deps =>
    import deps._

    val throttle = IntervalBasedThrottler.create[Int](1000.perSecond) // max 1000 messages per second -> 10 msg / tick (= 10ms)

    val flow = slowInfiniteSource(300.millis).via(throttle).runWith(TestSink.probe[Seq[Int]])

    val (time, intervals) = timeOfProcessing(11, 1).elementsOf(flow)
    time should be >= 2.7.seconds
    time should be <= 3.2.seconds
    intervals.foreach {
      _ should be >= 300.millis
    }

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

  protected def timeOfProcessing(n: Int, batchSize: Int) = new {

    private val initialOffset = 3 * batchSize
    private val defaultExpectedBatches: List[Seq[Int]] = (1 to n).inclusive.grouped(batchSize).toList

    def elementsOf(flow: TestSubscriber.Probe[Seq[Int]],
                   expectedBatches: List[Seq[Int]] = defaultExpectedBatches): (FiniteDuration, List[FiniteDuration]) = {
      // ask for an element, to discard stream initialization & warm up time from our measurement
      flow.request(initialOffset)
      flow.expectNextN((1 to initialOffset).inclusive.grouped(batchSize).toList)

      //      val startMillis = System.currentTimeMillis()
      flow.request(n)
      val timestamps = {
        val lastExpectedElement = expectedBatches.last.last + initialOffset

        @tailrec
        def collectTimestamps(acc: List[Long]): List[Long] = {
          val batch = flow.expectNext()
          val t = System.currentTimeMillis()
          println(s"received batch: $batch")
          val newAcc = t :: acc
          println(s"${batch.last} =?= $lastExpectedElement == ${batch.last == lastExpectedElement}")
          if (batch.last == lastExpectedElement) {
            newAcc.reverse
          } else {
            collectTimestamps(newAcc)
          }
        }

        collectTimestamps(Nil)
      }

      val intervals: List[FiniteDuration] = timestamps.sliding(2, 1).map {
        case List(a, b) => (b - a).millis
      }.toList

      println(s"Intervals: $intervals")

      val startMillis = timestamps.head
      val endMillis = timestamps.last

      //        expectedBatches.map { _ =>
      //        flow.expectN
      //        /*val arrivalTime = */flow.expectEventPF {
      //          case OnNext(xs) if xs == expectedBatch => System.currentTimeMillis().millis
      //        }
      //        (avgInterval + arrivalTime) / 2
      //      }.reduce((a, b) => (a + b) / 2)

      //      () map { _ =>

      //      }.reduceLeft { (acc, t) =>
      //        (acc + t) / 2
      //      }.millis

      //      flow.expectNextN(expectedBatches.map(_.map(_ + initialOffset)))
      //      val endMillis = System.currentTimeMillis()

      ((endMillis - startMillis).millis, intervals)
    }
  }

  protected def infiniteSourceWithRandomDelay(maxDelay: FiniteDuration): Source[Int, NotUsed] =
    slowInfiniteSource((Random.nextLong() % maxDelay.toMillis).millis + 1.milli)

  protected def infiniteSource: Source[Int, NotUsed] = Source(Stream.from(1, 1))

  protected def slowInfiniteSource(pushDelay: FiniteDuration): Source[Int, NotUsed] =
    infiniteSource.throttle(1, pushDelay, 1, ThrottleMode.shaping)

}

trait StreamTestingSandbox {

  protected def systemName: String

  protected def sandbox(test: StreamTestDependencies => Unit): Unit = {
    val testDependencies = new StreamTestDependencies(systemName)
    try {
      test(testDependencies)
    } finally {
      testDependencies.actorSystem.terminate()
      Await.ready(testDependencies.actorSystem.whenTerminated, 4.seconds)
    }
  }

  protected class StreamTestDependencies(systemName: String) {
    implicit val actorSystem: ActorSystem = ActorSystem(systemName)
    implicit val materializer: Materializer = ActorMaterializer()
  }

}
