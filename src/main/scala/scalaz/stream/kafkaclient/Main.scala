package scalaz.stream
package kafkaclient

import scalaz.concurrent.Task
import java.util.Properties
import kafka.consumer._
import kafka.serializer._
import scodec.bits.ByteVector
import java.util.concurrent.{Executors, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

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

    override def toString = s"key: ${keyAsString} value: ${valueAsString}"
  }

  object ByteVectorDecoder extends Decoder[ByteVector] {
    def fromBytes(bytes: Array[Byte]): ByteVector = ByteVector(bytes)
  }


  def createConsumer(zookeeper: List[String], gid: String): Task[ConsumerConnector] = {
    val p = new Properties()
    p.setProperty("zookeeper.connect", zookeeper.mkString(","))
    p.setProperty("group.id", gid)
    p.setProperty("zookeeper.session.timeout.ms", "400")
    p.setProperty("zookeeper.sync.time.ms", "200")
    p.setProperty("auto.commit.interval.ms", "1000")
    p.setProperty("auto.offset.reset", "smallest")
    val c = new ConsumerConfig(p)
    Task{ Consumer.create(c) }
  }

  def subscribe(c: Task[ConsumerConnector], topic: String, nPartitions: Int = 1): Process[Task, KeyedValue] = {
    val streams = c map ( consumer => consumer.createMessageStreams(Map(topic -> nPartitions), ByteVectorDecoder, ByteVectorDecoder)(topic))

    val ec = Executors.newFixedThreadPool(nPartitions, new NamedThreadFactory("KafkaClient"))

    val queue = async.boundedQueue[KeyedValue](10)

    def task(s: KafkaStream[ByteVector, ByteVector]) : Task[Unit] = Task.fork(Task {
      val it = s.iterator
      while(it.hasNext) {
        val next = it.next()
        queue.enqueueOne(KeyedValue(Option(next.key()), next.message())).run
      }
    })(ec)

    val p = Process.await( streams ) { strm =>
      val all = strm.foldLeft(Process.empty[Task, KeyedValue]){case (p, s) => p merge Process.eval_(task(s))}
      all merge queue.dequeue
    }

    p onComplete(Process eval_ c.map{ c =>
      c.shutdown()
      ec.shutdown()
    })
  }
}


object Main {
  import KafkaClient._
  import scala.util.Try
  def main(args: Array[String]) = {
    val zookeeper = args(0)
    val topic = args(1)
    val groupId = Try{args(2)}.toOption.getOrElse("meh")
    val numPartitions = Try{args(3).toInt}.toOption.getOrElse(1)

    val c = createConsumer(zookeeper.split(",").toList, groupId)
    val t = subscribe(c, topic, numPartitions)
    val p = t.map(println)
    p.run.run
  }

}
