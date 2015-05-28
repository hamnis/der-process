package scalaz.stream
package kafkaclient

import scalaz.concurrent.Task
import java.util.Properties
import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.serializer._
import scodec.bits.ByteVector

object KafkaClient {

  case class KeyedValue(key: Option[ByteVector], value: ByteVector) {
    def keyAsString = key.flatMap(_.decodeUtf8.right.toOption).getOrElse("")
    def valueAsString = value.decodeUtf8.right.getOrElse("")

    override def toString = s"key: ${keyAsString} value: ${valueAsString}"
  }

  object ByteVectorDecoder extends Decoder[ByteVector] {
    def fromBytes(bytes: Array[Byte]): ByteVector = ByteVector(bytes)
  }


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

  def subscribe(c: ConsumerConnector, topic: String): Process[Task, KeyedValue] = {
    val streams = c.createMessageStreams(Map(topic -> 1), ByteVectorDecoder, ByteVectorDecoder)(topic)

    def go(it: Iterator[MessageAndMetadata[ByteVector, ByteVector]]): Process0[KeyedValue] = {      
      if (it.hasNext) {
        val next = it.next()
        Process.emit(KeyedValue(Option(next.key()), next.message())) ++ go(it)
      } else Process.halt
    } 
    
    go(streams.head.iterator) onComplete(Process eval_ Task.delay(c.shutdown()))
  }
}


object Main {
  import KafkaClient._
  def main(args: Array[String]) = {
    val zookeeper = args(0)
    val topic = args(1)

    val c = createConsumer(zookeeper.split(",").toList, "meh")
    val t = subscribe(c, topic)

    t.map(println).run.run
  }

}
