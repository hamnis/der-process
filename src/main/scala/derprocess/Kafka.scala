package derprocess

import java.util

import scala.collection.JavaConverters._
import fs2._
import fs2.util.Async
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.language.higherKinds

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
  def subscribe[F[_]: Async, A : Deserializer, B: Deserializer](config: java.util.Properties, topic: String, timeout: Long): Stream[F, KeyedValue[A, B]] = {
    val F = Async[F]

    val client = F.delay{
      val client = new KafkaConsumer[A, B](config, implicitly[Deserializer[A]], implicitly[Deserializer[B]])
      client.subscribe(util.Arrays.asList(topic))
      client
    }

    def toKeyedValue(rec:ConsumerRecord[A, B]): List[KeyedValue[A, B]] =
      Option(rec.value()).map(v => KeyedValue(Option(rec.key()), v)).toList

    Stream.bracket(client)(c => {
      Stream.repeatEval(F.delay{
        val records = c.poll(timeout)
        records.records(topic).iterator().asScala.flatMap(toKeyedValue).toList
      }).flatMap(Stream.emits)
    }, c => F.delay(c.close()))
  }

  def sink[F[_]: Async, A, B](config: java.util.Properties, topic: String)(implicit aSerializer: Serializer[A], bSerialiser: Serializer[B]): Pipe[F, KeyedValue[A, B], RecordMetadata] = {
    stream => {
      val F = Async[F]

      val client = F.delay {
        new KafkaProducer[A, B](config, aSerializer, bSerialiser)
      }

      def send(p: KafkaProducer[A, B], topic: String, kv: KeyedValue[A, B]): F[RecordMetadata] = {
        F.async(either => {
          p.send(new ProducerRecord[A, B](topic, kv.key.getOrElse(null.asInstanceOf[A]), kv.value), new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
              if (exception != null) either(Left(exception)) else either(Right(metadata))
            }
          })
          F.pure(())
        })
      }


      def go(): Stream[F, RecordMetadata] = {
        val prod = Stream.bracket(client)(
          p => stream.evalMap(kv => send(p, topic, kv)),
          p => F.delay(p.close())
        )
        prod.onError(_ => go())
      }
      go()
    }
  }
}
