package com.softwaremill.akka.stream.throttle

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic._

import akka.event.Logging
import akka.stream._
import akka.stream.stage._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._

object IntervalBasedThrottler {

  // 10 millis is the default tick time for the Akka's Scheduler
  val MinTickTime: FiniteDuration = 10.millis

  def create[T](minInterval: FiniteDuration, maxBatchSize: Int): IntervalBasedThrottler[T] = new IntervalBasedThrottler(minInterval, maxBatchSize)

}

class IntervalBasedThrottler[T](val interval: FiniteDuration, val maxBatchSize: Int) extends GraphStage[FlowShape[T, immutable.Seq[T]]] {

  require(interval >= IntervalBasedThrottler.MinTickTime, s"Interval should be >= ${IntervalBasedThrottler.MinTickTime}")

  val in: Inlet[T] = Inlet[T](Logging.simpleName(this) + ".in")

  val out: Outlet[immutable.Seq[T]] = Outlet[immutable.Seq[T]](Logging.simpleName(this) + ".out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {

    private val stash = new ConcurrentLinkedQueue[T]()
    // Because ConcurrentLinkedQueue.size has weak consistency (especially while another thread
    // adds/removes elements) and requires O(n) traversal, it's better to keep an atomic counter instead.
    private val stashSize = new AtomicInteger(0)
    private val isFinished = new AtomicBoolean(false)

    setHandlers(in, out, this)

    override def preStart(): Unit = {
      scheduleOnce("IntervalBasedThrottlerTimer", interval)
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

    override protected def onTimer(timerKey: Any): Unit = {
      emitBatch()
      scheduleOnce("IntervalBasedThrottlerTimer", interval)
    }

    protected def emitBatch(): Unit = {
      if (isAvailable(out)) {
        val batch = pop(maxBatchSize, Nil)
        if (batch != Nil) {
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
      if (n <= 0) {
        acc.reverse
      } else {
        val e = stash.poll()
        if (e == null) {
          acc.reverse
        } else {
          stashSize.decrementAndGet()
          pop(n - 1, e :: acc)
        }
      }
    }

  }

}
