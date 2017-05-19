package com.softwaremill.akka.stream.throttle

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.event.Logging
import akka.stream.Attributes.InputBuffer
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.contrib.Pulse
import akka.stream.stage._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

object IntervalBasedThrottler {

  val TickTime: FiniteDuration = 10.millis

  def create[T](throttleSettings: IntervalBasedThrottlerSettings): Graph[FlowShape[T, Seq[T]], NotUsed] = {

    import GraphDSL.Implicits._

    val (batchSize, interval) = calculateBatchSizeAndInterval(throttleSettings)

    println(s"batchSize = $batchSize, interval = $interval")

    GraphDSL.create() { implicit builder =>
      //      val ticksSource = builder.add(Source.tick(interval, interval, ()))
      // groupedWithin(batchSize, interval)
      val groupWithin = builder.add(Flow[T].map(e => List(e))/*groupedWithin(batchSize, interval).*/.via(new TimerGate(interval)))
      //      val pulse = builder.add(new Pulse(interval, false))
      //      val zip = builder.add(ZipWith[T, Unit, T]((in0, _) => in0))

      //      ticksSource ~> zip.in1
      //      groupWithin ~> zip.in0
      //      groupWithin ~> pulse.in

      //      FlowShape(groupWithin.in, zip.out)
      //      FlowShape(zip.in0, zip.out.map(List(_)).outlet)
      FlowShape(groupWithin.in, groupWithin.out)
    }
  }

  private def calculateBatchSizeAndInterval(throttleSettings: IntervalBasedThrottlerSettings): (Int, FiniteDuration) = {
    val elements = throttleSettings.numberOfOps
    val per = throttleSettings.duration

    @tailrec
    def findGcd(p: Int, q: Int): Int = {
      if (q == 0) p
      else findGcd(q, p % q)
    }

    val numOfTicks = (BigDecimal(per.toMillis) / TickTime.toMillis).setScale(0, RoundingMode.DOWN).toInt
    val gcd = findGcd(numOfTicks max elements, numOfTicks min elements)

    if (per / gcd >= TickTime) {
      (elements / gcd, (per / gcd) + 1.milli)
    } else {
      (elements, per)
    }
  }

}

class TimerGate[T](val interval: FiniteDuration) extends GraphStage[FlowShape[T, T]] {

  val in = Inlet[T](Logging.simpleName(this) + ".in")

  val out = Outlet[T](Logging.simpleName(this) + ".out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with InHandler with OutHandler {

    private val pending = new AtomicLong(0L)

    setHandlers(in, out, this)

    override def preStart(): Unit = schedulePeriodicallyWithInitialDelay("TimerGateTimer", interval, interval)

    override def onPush(): Unit = pending.incrementAndGet()

    override def onPull(): Unit = if (!isClosed(in)) pull(in)

    override protected def onTimer(timerKey: Any): Unit = {
      if (timerKey == "TimerGateTimer") {

        println(s"${DateTimeFormatter.ISO_LOCAL_TIME.format(LocalDateTime.now())} - TICK!")
        val p = pending.get()
        if (p > 0 && isAvailable(out) && !isClosed(in)) {
          if (pending.compareAndSet(p, p - 1)) {
            push(out, grab(in))
          }
        }
//        scheduleOnce("TimerGateTimer", interval)
      }
    }

  }

}

