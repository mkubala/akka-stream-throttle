package com.softwaremill.akka.stream.throttle

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber.{OnComplete, OnNext}
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class IntervalBasedThrottlerSpec extends FlatSpec with ParallelTestExecution with Matchers
  with IntervalBasedThrottlerTestKit {

  it should "limit rate of messages - low frequency" in test(
    source = infiniteSource,
    numOfElements = 6,
    maxBatchSize = 1,
    minInterval = 500.millis
  )

  it should "limit rate of messages - medium frequency, 10ms interval" in test(
    source = infiniteSource,
    numOfElements = 300,
    maxBatchSize = 1,
    minInterval = 10.millis
  )

  it should "limit rate of messages - medium frequency, 100ms interval" in test(
    source = infiniteSource,
    numOfElements = 300,
    maxBatchSize = 10,
    minInterval = 100.millis
  )

  it should "limit rate of messages - medium frequency, 100ms interval, source with random delay 1-100ms" in test(
    source = infiniteSourceWithRandomDelay(100.millis),
    numOfElements = 300,
    maxBatchSize = 10,
    minInterval = 100.millis
  )

  it should "limit rate of messages - moderate frequency, 10ms interval" in test(
    source = infiniteSource,
    numOfElements = 600,
    maxBatchSize = 2,
    minInterval = 10.millis
  )

  it should "limit rate of messages - moderate frequency, 100ms interval" in test(
    source = infiniteSource,
    numOfElements = 600,
    maxBatchSize = 20,
    minInterval = 100.millis
  )

  it should "limit rate of messages - high frequency, 10ms interval" in test(
    source = infiniteSource,
    numOfElements = 6000,
    maxBatchSize = 20,
    minInterval = 10.millis
  )

  it should "limit rate of messages - high frequency, 100ms interval" in test(
    source = infiniteSource,
    numOfElements = 6000,
    maxBatchSize = 200,
    minInterval = 100.millis
  )

  it should "limit rate of messages - high frequency, 1000ms interval" in test(
    source = infiniteSource,
    numOfElements = 6000,
    maxBatchSize = 2000,
    minInterval = 1000.millis
  )

  it should "limit rate of messages - extremely high frequency, 10ms interval" in test(
    source = infiniteSource,
    numOfElements = 150000,
    maxBatchSize = 500,
    minInterval = 10.millis
  )

  it should "limit rate of messages - extremely high frequency, 1000ms interval" in test(
    source = infiniteSource,
    numOfElements = 150000,
    maxBatchSize = 50000,
    minInterval = 1000.millis
  )

  it should "keep limits with slow producer" in test(
    source = slowInfiniteSource(300.millis),
    numOfElements = 10,
    maxBatchSize = 1,
    minInterval = 100.millis
  )

}

trait IntervalBasedThrottlerTestKit extends BeforeAndAfterAll {
  this: Suite with Matchers =>

  implicit val system: ActorSystem = ActorSystem("ThrottleSpec-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  override protected def afterAll(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 5.seconds)
    super.afterAll()
  }

  type Batch = Seq[Int]

  def test(source: => Source[Int, _],
           numOfElements: Int,
           maxBatchSize: Int,
           minInterval: FiniteDuration): Unit = {

    val flow = source
      .take(numOfElements)
      .via(IntervalBasedThrottler.create[Int](minInterval, maxBatchSize))
      .map { batch =>
        (System.currentTimeMillis(), batch)
      }.runWith(TestSink.probe[(Long, Seq[Int])])

    val (timestamps, batches) = {

      def collectTimestampsAndBatches(acc: List[(Long, Batch)]): List[(Long, Batch)] = {
        flow.request(1)
        flow.expectEventPF {
          case OnNext(batch) if batch.isInstanceOf[(Long, Batch)] =>
            collectTimestampsAndBatches(batch.asInstanceOf[(Long, Batch)] :: acc)
          case OnComplete | _ =>
            acc.reverse
        }
      }

      collectTimestampsAndBatches(Nil)
    }.unzip

    val intervals: Seq[FiniteDuration] = timestamps.sliding(2, 1).map {
      case List(first, second) => (second - first).millis
    }.toList

    intervals.foreach {
      _ should be >= minInterval
    }

    batches.flatten should contain theSameElementsInOrderAs (1 to numOfElements).inclusive
    batches.size shouldBe (numOfElements / maxBatchSize)
  }

  protected def infiniteSourceWithRandomDelay(maxDelay: FiniteDuration): Source[Int, NotUsed] = {
    def delay: FiniteDuration = (Math.abs(Random.nextLong()) % maxDelay.toMillis).millis + 100.milli

    Source.apply(Stream.iterate(1) { a =>
      val d = delay
      Thread.sleep(d.toMillis)
      a + 1
    })
  }

  protected def infiniteSource: Source[Int, NotUsed] = Source(Stream.from(1, 1))

  protected def slowInfiniteSource(pushDelay: FiniteDuration): Source[Int, NotUsed] =
    infiniteSource.throttle(1, pushDelay, 1, ThrottleMode.shaping)

}
