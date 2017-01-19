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
