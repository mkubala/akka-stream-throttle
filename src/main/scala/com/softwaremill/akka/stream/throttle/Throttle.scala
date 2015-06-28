package com.softwaremill.akka.stream.throttle

import akka.actor.Cancellable
import akka.stream.Attributes
import akka.stream.scaladsl._

import scala.concurrent.duration._

object Throttle {

  import FlowGraph.Implicits._

  private val AkkaSchedulerInterval = 10.millis

  def create[T](throttleSettings: ThrottleSettings): Flow[T, (T, Unit), Unit]#Repr[T, Unit] =
    Flow() { implicit builder =>
      val zip = builder.add(Zip[T, Unit]())
      ticksSource(throttleSettings) ~> zip.in1
      (zip.in0, zip.out)
    }.map(_._1).withAttributes(Attributes.inputBuffer(initial = 1, max = 1)).named("throttle")

  private def ticksSource(throttleSettings: ThrottleSettings): Source[Unit, Cancellable] =
  // Akka scheduler is lightweight but has limited frequency (up to 1ms). By default it's 10ms,
  // which would allow us to pass max 100 msg/s. Thus in case of desirable throttle throughput
  // greater than 100 msg/s we are passing more than one tick at a time (e.g. if you need to
  // send 200 msg/s, then this tick source will emit 2 ticks in 10ms intervals, instead of trying
  // to send 1 tick / 5ms).
    if (throttleSettings.interval < AkkaSchedulerInterval) {
      val factor: Int = (10.millis.toNanos / throttleSettings.interval.toNanos).toInt
      val tickSource = Source(Duration.Zero, throttleSettings.interval * factor, ())
      tickSource.mapConcat(_ => List.fill(factor)(()))
    } else {
      Source(Duration.Zero, throttleSettings.interval, ())
    }

}