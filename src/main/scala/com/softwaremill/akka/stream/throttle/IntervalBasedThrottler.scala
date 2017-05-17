package com.softwaremill.akka.stream.throttle

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

object IntervalBasedThrottler {

  val TickTime: FiniteDuration = 100.millis

  def create[T](throttleSettings: IntervalBasedThrottlerSettings): Graph[FlowShape[T, Seq[T]], NotUsed] = {

    import GraphDSL.Implicits._

    val (batchSize, interval) = calculateBatchSizeAndInterval(throttleSettings)

    GraphDSL.create() { implicit builder =>
      val ticksSource = builder.add(Source.tick(interval, interval, ()))
//      val groupWithin = builder.add(Flow[T].groupedWithin(batchSize, interval))
      val zip = builder.add(ZipWith[T, Unit, T]((in0, _) => in0))

      ticksSource ~> zip.in1
//      groupWithin ~> zip.in0

//      FlowShape(groupWithin.in, zip.out)
      FlowShape(zip.in0, zip.out.map(List(_)).outlet)
//      FlowShape(groupWithin.in, groupWithin.out)
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
