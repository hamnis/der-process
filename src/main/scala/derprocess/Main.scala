package derprocess

import Implicits._
import fs2._

import scala.util.Try

object Main {
  def main(args: Array[String]) = {
    val bootstrapServers = args(0)
    val topic = args(1)
    val groupId = Try{args(2)}.toOption.getOrElse("meh")

    val brokers = bootstrapServers.split(",").toList.flatMap(s => Broker(s).toList)
    val config = KafkaConsumerConfig(
      brokers,
      groupId
    )

    implicit val strategy: Strategy = Strategy.fromFixedDaemonPool(4)

    val c = Kafka.subscribe[Task, String, String](config.properties, topic, 5000L)

    val sink = Kafka.sink[Task, String, String](KafkaProducerConfig(brokers).properties, topic + "1")

    val p = c.flatMap(e => sink(Stream.emit(e)).map(md => e -> md)).map(println)
    p.run.unsafeRun()
  }
}
