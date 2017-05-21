package com.softwaremill.akka.stream.throttle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.NotUsed
import akka.event.Logging
import akka.stream._
import akka.stream.stage._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

object IntervalBasedThrottler {

  val MinTickTime: FiniteDuration = 10.millis

  def create[T](throttleSettings: IntervalBasedThrottlerSettings): Graph[FlowShape[T, immutable.Seq[T]], NotUsed] = {
    val (batchSize, interval) = calculateBatchSizeAndInterval(throttleSettings)
    new IntervalBasedThrottler(interval, batchSize)
  }

  def create[T](minInterval: FiniteDuration, maxBatchSize: Int): IntervalBasedThrottler[T] = new IntervalBasedThrottler(minInterval, maxBatchSize)

  private def calculateBatchSizeAndInterval(throttleSettings: IntervalBasedThrottlerSettings): (Int, FiniteDuration) = {
    val elements = throttleSettings.numberOfOps
    val per = throttleSettings.duration

    @tailrec
    def findGcd(p: Int, q: Int): Int = {
      if (q == 0) p
      else findGcd(q, p % q)
    }

    val numOfTicks = (BigDecimal(per.toMillis) / MinTickTime.toMillis).setScale(0, RoundingMode.DOWN).toInt
    val gcd = findGcd(numOfTicks max elements, numOfTicks min elements)

    if (per / gcd >= MinTickTime) {
      (elements / gcd, (per / gcd) + 1.milli)
    } else {
      (elements, per)
    }
  }

}

class IntervalBasedThrottler[T](val interval: FiniteDuration, val maxBatchSize: Int) extends GraphStage[FlowShape[T, immutable.Seq[T]]] {

  val in = Inlet[T](Logging.simpleName(this) + ".in")

  val out = Outlet[immutable.Seq[T]](Logging.simpleName(this) + ".out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {

    private val stash = new ConcurrentLinkedQueue[T]()
    // Because ConcurrentLinkedQueue.size has weak consistency (especially while another thread
    // adds/removes elements) and requires O(n) traversal, it's better to keep an atomic counter instead.
    private val stashSize = new AtomicInteger(0)
    private val isFinished = new AtomicBoolean(false)

    setHandlers(in, out, this)

    override def preStart(): Unit = {
      println(s"interval = $interval")
      scheduleOnce("TimerGateTimer", interval)
      tryPull()
    }

    override def onPush(): Unit = {
      stash.add(grab(in))
      stashSize.incrementAndGet()
      if (stashSize.get < maxBatchSize) {
        tryPull()
      }
    }

    override def onPull(): Unit = tryPull()

    // TODO throughput improvement:
    // - migrate to schedule periodically & ignore missed ticks
    // OR
    // - increase the TickTime (bigger batches)
    override protected def onTimer(timerKey: Any): Unit = {
      if (timerKey == "TimerGateTimer") {
        scheduleOnce("TimerGateTimer", interval)
        emitBatch()
      }
    }

    protected def emitBatch(): Unit = {
      if (isAvailable(out)) {
        val batch = pop(maxBatchSize, Nil)
        if (batch != Nil) {
          println(s"${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())} - emitBatch")
          push(out, batch)
        } else if (isFinished.get()) {
          completeStage()
        }
      }
      tryPull()
    }


    override def onUpstreamFinish(): Unit = isFinished.set(true)

    @inline
    private def tryPull(): Unit =
      if (!hasBeenPulled(in) && !isFinished.get()) {
        pull(in)
      }

    @tailrec
    private def pop(n: Int, acc: List[T]): List[T] = {
      if (n == 0) {
        acc.reverse
      } else {
        val e = stash.poll()
        stashSize.decrementAndGet()
        if (e == null) {
          acc.reverse
        } else {
          pop(n - 1, e :: acc)
        }
      }
    }

  }

}
