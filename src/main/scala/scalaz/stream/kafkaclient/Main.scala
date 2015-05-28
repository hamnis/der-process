package scalaz.stream
package kafkaclient

import scalaz.concurrent.Task
import java.util.Properties
import kafka.consumer._
import kafka.serializer._
import scodec.bits.ByteVector
import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scalaz.\/

object KafkaClient {

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

  case class KeyedValue(key: Option[ByteVector], value: ByteVector) {
    def keyAsString = key.flatMap(_.decodeUtf8.right.toOption).getOrElse("")
    def valueAsString = value.decodeUtf8.right.getOrElse("")

    override def toString = s"key: ${keyAsString} ${valueAsString}"
  }

  object ByteVectorDecoder extends Decoder[ByteVector] {
    def fromBytes(bytes: Array[Byte]): ByteVector = ByteVector(bytes)
  }

  case class StreamConfig(nStream: Int = 1, queueSize: Int = 10, exHandler: Throwable \/ List[Unit] => Unit = println)


  def createConsumer(zookeeper: List[String], gid: String): ConsumerConnector = {
    val p = new Properties()
    p.setProperty("zookeeper.connect", zookeeper.mkString(","))
    p.setProperty("group.id", gid)
    p.setProperty("zookeeper.session.timeout.ms", "400")
    p.setProperty("zookeeper.sync.time.ms", "200")
    p.setProperty("auto.commit.interval.ms", "1000")
    p.setProperty("auto.offset.reset", "smallest")
    val c = new ConsumerConfig(p)
    Consumer.create(c)
  }

  def subscribe(c: ConsumerConnector, topic: String): StreamConfig => Process[Task, KeyedValue] = { case StreamConfig(count, queueSize, exHandler) =>
    val ec = Executors.newFixedThreadPool(count, new NamedThreadFactory("KafkaClient"))

    val streams = c.createMessageStreams(Map(topic -> count), ByteVectorDecoder, ByteVectorDecoder)(topic)
    val queue = async.mutable.Queue[KeyedValue](queueSize)

    def enqueue(s: KafkaStream[ByteVector, ByteVector]) = {
      val it = s.iterator
      while (it.hasNext) {
        val next = it.next
        queue.enqueueOne(KeyedValue(Option(next.key()), next.message())).run
      }
    } 
    
    val t = Task.gatherUnordered(streams.map(s => Task.delay(enqueue(s))))
    Task.fork(t)(ec).runAsync(exHandler)
    queue.dequeue.onComplete(Process eval_ Task.delay{
          c.shutdown()
          ec.shutdown() 
    })    
  }
}


object Main {
  import KafkaClient._
  def main(args: Array[String]) = {
    val zookeeper = args(0)
    val topic = args(1)

    val c = createConsumer(zookeeper.split(",").toList, "meh")
    val t = subscribe(c, topic)(StreamConfig())

    t.map(println).run.run    
  }

}
