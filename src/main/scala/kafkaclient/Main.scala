package kafkaclient

import Kafka._
import scala.util.Try

object Main {
  def main(args: Array[String]) = {
    val zookeeper = args(0)
    val topic = args(1)
    val groupId = Try{args(2)}.toOption.getOrElse("meh")
    val numPartitions = Try{args(3).toInt}.toOption.getOrElse(1)

    val c = consumer(zookeeper.split(",").toList, groupId)
    val t = subscribe(c, topic, numPartitions)
    val p = t.map(println)
    p.run.run
  }
}
