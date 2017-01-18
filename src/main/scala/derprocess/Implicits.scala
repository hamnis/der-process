package derprocess


import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes

object Implicits {
  implicit val StringDeserializer: Deserializer[String] = new StringDeserializer
  implicit val bytesDeserialiser: Deserializer[Bytes] = new BytesDeserializer
  implicit val byteArrayDeserialiser: Deserializer[Array[Byte]] = new ByteArrayDeserializer
}
