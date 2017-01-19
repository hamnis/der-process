package derprocess


import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes

object Implicits {
  implicit val StringDeserializer: Deserializer[String] = new StringDeserializer
  implicit val StringSerializer: Serializer[String] = new StringSerializer

  implicit val bytesDeserialiser: Deserializer[Bytes] = new BytesDeserializer
  implicit val bytesSerialiser: Serializer[Bytes] = new BytesSerializer

  implicit val byteArrayDeserialiser: Deserializer[Array[Byte]] = new ByteArrayDeserializer
  implicit val byteArraySerializer: Serializer[Array[Byte]] = new ByteArraySerializer
}
