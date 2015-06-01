package kafkaclient

import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger


class NamedThreadFactory(name: String, daemon: Boolean = true) extends ThreadFactory {
  val default = Executors.defaultThreadFactory()
  val counter = new AtomicInteger(1)
  def newThread(r: Runnable) = {
    val t = default.newThread(r)
    t.setName(name + "-" + counter.getAndIncrement)
    t.setDaemon(daemon)
    t
  }
}
