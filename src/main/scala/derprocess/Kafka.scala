package derprocess

import scala.collection.JavaConverters._
import fs2._
import fs2.util.Async
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer._

import scala.language.higherKinds

object Kafka {
  def subscribe[F[_]: Async, A : KDeserializer, B: KDeserializer](config: Map[String, AnyRef], topic: String, timeout: Long): Stream[F, ConsumerRecord[A, B]] = {
    val F = Async[F]

    val client = F.delay{
      val client = new KafkaConsumer[A, B](config.asJava, KDeserializer[A].key(config), KDeserializer[B].value(config))
      client.subscribe(java.util.Arrays.asList(topic))
      client
    }

    Stream.bracket(client)(c => {
      Stream.repeatEval(F.delay{
        val records = c.poll(timeout)
        records.records(topic).iterator().asScala.toList
      }).flatMap(Stream.emits)
    }, c => F.delay(c.close()))
  }

  def sink[F[_]: Async, A : KSerializer, B : KSerializer](config: Map[String, AnyRef], topic: String): Pipe[F, ConsumerRecord[A, B], RecordMetadata] = {
    stream => {
      val F = Async[F]

      val client = F.delay {
        new KafkaProducer[A, B](config.asJava, KSerializer[A].key(config), KSerializer[B].value(config))
      }

      def send(p: KafkaProducer[A, B], topic: String, kv: ConsumerRecord[A, B]): F[RecordMetadata] = {
        F.async(onComplete => {
          p.send(new ProducerRecord[A, B](topic, kv.key(), kv.value()), new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
              if (exception != null) onComplete(Left(exception)) else onComplete(Right(metadata))
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
