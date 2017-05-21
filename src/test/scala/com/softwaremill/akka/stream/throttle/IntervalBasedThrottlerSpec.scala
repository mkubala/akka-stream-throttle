package com.softwaremill.akka.stream.throttle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.TestSubscriber.{OnComplete, OnNext}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream._
import com.softwaremill.akka.stream.throttle.IntervalBasedThrottlerSettings._
import org.scalatest.{FlatSpec, Matchers, ParallelTestExecution}

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

class IntervalBasedThrottlerSpec extends FlatSpec /*with ParallelTestExecution*/ with Matchers
  with IntervalBasedThrottlerTestKit with StreamTestingSandbox {

  val TimeTolerance: FiniteDuration = 50.millis

  val systemName = "ThrottleSpec-system"

  //  it should "limit rate of messages - low frequency" in sandbox { deps =>
  //    import deps._
  //
  //    val throttle = IntervalBasedThrottler.create[Int](2.perSecond)
  //
  //    val flow = infiniteSource.via(throttle).runWith(TestSink.probe[Seq[Int]])
  //
  //    val (time, avgInterval) = timeOfProcessing(6, 1).elementsOf(flow)
  //    time should be >= 2.5.seconds
  //    time should be <= 3.seconds
  //
  //    flow.cancel()
  //  }

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

  it should "limit rate of messages - high frequency" in sandbox { deps =>
    import deps._

    val source = infiniteSource
    val throttle = IntervalBasedThrottler.create[Int](2000.perSecond) // max 2000 messages per second

    val flow = source.take(6000).via(throttle).runWith(TestSink.probe[Seq[Int]])

    val (time, intervals, batches) = drain(flow)
    time should be >= (6000 / 20) * 10.millis
    time should be <= (6000 / 20) * 40.millis * 1.3
    intervals.foreach {
      _ should be >= 10.millis
    }
    batches.flatten should contain theSameElementsInOrderAs (1 to 6000).inclusive
    batches.size shouldBe (6000 / 20)
  }

  it should "limit rate of messages - extremely high frequency" in sandbox { deps =>
    import deps._

    val source = infiniteSource
    val throttle = IntervalBasedThrottler.create[Int](50000.perSecond) // max 2000 messages per second

    val flow = source.take(150000).via(throttle).runWith(TestSink.probe[Seq[Int]])

    val (time, intervals, batches) = drain(flow)
    time should be >= 300 * 10.millis
    time should be <= 300 * 40.millis * 1.3
    intervals.foreach {
      _ should be >= 10.millis
    }
    batches.flatten should contain theSameElementsInOrderAs (1 to 150000).inclusive
    batches.size shouldBe 300
  }

  it should "limit rate of messages - medium frequency" in sandbox { deps =>
    import deps._

    val source = infiniteSource
    val throttle = IntervalBasedThrottler.create[Int](100.millis, 10)
    val flow = source.take(300).via(throttle).runWith(TestSink.probe[Seq[Int]])

    val (time, intervals, batches) = drain(flow)
    time should be >= 30 * 100.millis
    time should be <= 30 * 100.millis * 1.3
    intervals.foreach {
      _ should be >= 100.millis
    }
    batches.flatten should contain theSameElementsInOrderAs (1 to 300).inclusive
    batches.size shouldBe 30
  }

  it should "keep limits with slow producer" in sandbox { deps =>
    import deps._

    val source = slowInfiniteSource(300.millis)
    val flow = source.take(10).via(IntervalBasedThrottler.create[Int](100.millis, 1)).runWith(TestSink.probe[Seq[Int]])

    val (time, intervals, batches) = drain(flow)
    time should be >= 10 * 100.millis
    time should be <= 3.5.seconds
    intervals.foreach {
      _ should be >= 100.millis
    }
    batches.flatten should contain theSameElementsInOrderAs (1 to 10).inclusive
    batches.size shouldBe 10
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

  type Batch = Seq[Int]

  protected def drain(flow: TestSubscriber.Probe[Seq[Int]]): (FiniteDuration, List[FiniteDuration], List[Batch]) = {
    val timestampsAndBatches = {

      def collectTimestampsAndBatches(acc: List[(Long, Batch)]): List[(Long, Batch)] = {
        flow.request(1)
        flow.expectEventPF {
          case OnNext(batch) if batch.isInstanceOf[Batch] =>
            println(s"received batch: $batch")
            val t: (Long, Batch) = (System.currentTimeMillis(), batch.asInstanceOf[Batch])
            collectTimestampsAndBatches(t :: acc)
          case OnComplete | _ =>
            acc.reverse
        }
      }

      collectTimestampsAndBatches(Nil)
    }

    val (timestamps, batches) = timestampsAndBatches.unzip
    val intervals: List[FiniteDuration] = timestamps.sliding(2, 1).map {
      case List(a, b) => (b - a).millis
    }.toList

    println(s"Intervals: $intervals")

    val startMillis = timestamps.head
    val endMillis = timestamps.last

    ((endMillis - startMillis).millis, intervals, batches)
  }

  protected def infiniteSourceWithRandomDelay(maxDelay: FiniteDuration): Source[Int, NotUsed] =
    slowInfiniteSource((Random.nextLong() % maxDelay.toMillis).millis + 1.milli)

  protected def infiniteSource: Source[Int, NotUsed] = Source(Stream.from(1, 1))

  protected def slowInfiniteSource(pushDelay: FiniteDuration): Source[Int, NotUsed] =
    infiniteSource.throttle(1, pushDelay, 1, ThrottleMode.shaping).map { e =>
      println(s"${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())} - source emits: $e")
      e
    }

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
