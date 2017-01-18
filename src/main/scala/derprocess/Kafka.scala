package derprocess

import java.util

import scala.collection.JavaConverters._
import fs2._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final case class Broker(host: String, port: Int = 9092)

object Broker {
  def apply(s: String): Option[Broker] = {
    s.split(":") match {
      case Array(host, port) => Some(Broker(host, port.toInt))
      case Array(host) => Some(Broker(host))
      case _ => None
    }
  }
}

final case class KafkaConsumerConfig(brokers: List[Broker], groupId: String, sessionTimeoutMs: Option[Long] = None, maxPollRecords: Option[Int] = None) {
  def properties: java.util.Properties = {
    import ConsumerConfig._

    val props = new java.util.Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers.map(b => s"${b.host}:${b.port}").mkString(","))
    props.put(GROUP_ID_CONFIG, groupId)
    sessionTimeoutMs.foreach(l => props.put(SESSION_TIMEOUT_MS_CONFIG, l.toString))
    maxPollRecords.foreach(v => props.put(MAX_POLL_RECORDS_CONFIG, v.toString))
    props
  }
}

final case class KafkaProducerConfig(brokers: List[Broker], lingerTimeoutMs: Option[Long] = None) {
  def properties: java.util.Properties = {
    import ProducerConfig._

    val props = new java.util.Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, brokers.map(b => s"${b.host}:${b.port}").mkString(","))
    lingerTimeoutMs.foreach(l => props.put(LINGER_MS_CONFIG, l.toString))
    props
  }
}

final case class KeyedValue[A, B](key: Option[A], value: B)

object Kafka {
  def subscribe[A: Deserializer, B: Deserializer](config: KafkaConsumerConfig, topic: String, timeout: Long): Stream[Task, KeyedValue[A, B]] = {
    subscribe(config.properties, topic, timeout)
  }

  def subscribe[A: Deserializer, B: Deserializer](config: java.util.Properties, topic: String, timeout: Long): Stream[Task, KeyedValue[A, B]] = {
    val client = Task.delay{
      val client = new KafkaConsumer[A, B](config, implicitly[Deserializer[A]], implicitly[Deserializer[B]])
      client.subscribe(util.Arrays.asList(topic))
      client
    }

    def toKeyedValue(rec:ConsumerRecord[A, B]): List[KeyedValue[A, B]] =
      Option(rec.value()).map(v => KeyedValue(Option(rec.key()), v)).toList

    Stream.bracket(client)(c => {
      Stream.repeatEval(Task.delay{
        val records = c.poll(timeout)
        records.records(topic).iterator().asScala.flatMap(toKeyedValue).toList
      }).flatMap(Stream.emits)
    }, c => Task.delay(c.close()))
  }

  def sink[A >: AnyRef : Serializer, B: Serializer](config: java.util.Properties, topic: String)(implicit strategy: Strategy): Sink[Task, KeyedValue[A, B]] = {
    stream => {
      val client = Task.delay{
        new KafkaProducer[A, B](config, implicitly[Serializer[A]], implicitly[Serializer[B]])
      }

      def send(p: KafkaProducer[A, B], topic: String, kv: KeyedValue[A, B])(implicit strategy: Strategy): Task[Unit] = {
        Task.async(either => p.send(new ProducerRecord[A, B](topic, kv.key.orNull, kv.value), new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
            if (exception != null) either(Left(exception)) else either(Right(()))
          }
        }))
      }


      def go(): Stream[Task, Unit] = {
        val prod = Stream.bracket(client)(
          p => stream.evalMap(kv => send(p, topic, kv)),
          p => Task.delay(p.close())
        )
        prod.onError(_ => go())
      }
      go()
    }

  }

}
