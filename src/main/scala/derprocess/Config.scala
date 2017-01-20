package derprocess

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig


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
  def asMap: Map[String, AnyRef] = {
    import ConsumerConfig._

    val props = Map[String, AnyRef](
      (BOOTSTRAP_SERVERS_CONFIG, brokers.map(b => s"${b.host}:${b.port}").mkString(",")),
      (GROUP_ID_CONFIG, groupId)
    )

    props ++ (sessionTimeoutMs.map(l => (SESSION_TIMEOUT_MS_CONFIG, l.toString)).toList ::: maxPollRecords.map(v => (MAX_POLL_RECORDS_CONFIG, v.toString)).toList)
  }
}

final case class KafkaProducerConfig(brokers: List[Broker], lingerTimeoutMs: Option[Long] = None) {
  def asMap: Map[String, AnyRef] = {
    import ProducerConfig._

    val m = Map[String, AnyRef](
      (BOOTSTRAP_SERVERS_CONFIG, brokers.map(b => s"${b.host}:${b.port}").mkString(","))
    )
    m ++ lingerTimeoutMs.map(l => (LINGER_MS_CONFIG, l.toString)).toList
  }
}
