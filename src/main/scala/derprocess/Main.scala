package derprocess

import Implicits._

import scala.util.Try

object Main {
  def main(args: Array[String]) = {
    val bootstrapServers = args(0)
    val topic = args(1)
    val groupId = Try{args(2)}.toOption.getOrElse("meh")

    val config = KafkaConfig(
      bootstrapServers.split(",").toList.flatMap(s => Broker(s).toList),
      groupId
    )

    val c = Kafka.subscribe[String, String](config, topic, 5000)

    val p = c.map(println)
    p.run.unsafeRun()
  }
}
