package derprocess

import java.nio.ByteBuffer

import org.apache.kafka.common.serialization.{Deserializer, Serdes, Serializer}
import org.apache.kafka.common.utils.Bytes

import scala.collection.JavaConverters._

trait KSerializer[A] {
  def key(config: Map[String, AnyRef]): Serializer[A]
  def value(config: Map[String, AnyRef]): Serializer[A]
}

object KSerializer {
  def apply[A](implicit ks: KSerializer[A]): KSerializer[A] = ks

  def from[A](serde: => Serializer[A]): KSerializer[A] = new KSerializer[A] {
    override def key(config: Map[String, AnyRef]) = {
      val ser = serde
      ser.configure(config.asJava, true)
      ser
    }

    override def value(config: Map[String, AnyRef]) = {
      val ser = serde
      ser.configure(config.asJava, false)
      ser
    }
  }


  implicit val StringSerializer: KSerializer[String] = from(Serdes.String().serializer())
  implicit val BytesSerializer: KSerializer[Bytes] = from(Serdes.Bytes().serializer())
  implicit val ByteArraySerializer: KSerializer[Array[Byte]] = from(Serdes.ByteArray().serializer())
  implicit val ByteBufferSerializer: KSerializer[ByteBuffer] = from(Serdes.ByteBuffer().serializer())
  implicit val DoubleSerializer: KSerializer[java.lang.Double] = from(Serdes.Double().serializer())
  implicit val IntegerSerializer: KSerializer[java.lang.Integer] = from(Serdes.Integer().serializer())
  implicit val LongSerializer: KSerializer[java.lang.Long] = from(Serdes.Long().serializer())
}

trait KDeserializer[A] {
  def key(config: Map[String, AnyRef]): Deserializer[A]
  def value(config: Map[String, AnyRef]): Deserializer[A]
}

object KDeserializer {
  def apply[A](implicit ks: KDeserializer[A]): KDeserializer[A] = ks

  def from[A](serde: => Deserializer[A]): KDeserializer[A] = new KDeserializer[A] {
    override def key(config: Map[String, AnyRef]) = {
      val ser = serde
      ser.configure(config.asJava, true)
      ser
    }

    override def value(config: Map[String, AnyRef]) = {
      val ser = serde
      ser.configure(config.asJava, false)
      ser
    }
  }

  implicit val StringDeserializer: KDeserializer[String] = from(Serdes.String().deserializer())
  implicit val BytesDeserializer: KDeserializer[Bytes] = from(Serdes.Bytes().deserializer())
  implicit val ByteArrayDeserializer: KDeserializer[Array[Byte]] = from(Serdes.ByteArray().deserializer())
  implicit val ByteBufferDeserializer: KDeserializer[ByteBuffer] = from(Serdes.ByteBuffer().deserializer())
  implicit val DoubleDeserializer: KDeserializer[java.lang.Double] = from(Serdes.Double().deserializer())
  implicit val IntegerDeserializer: KDeserializer[java.lang.Integer] = from(Serdes.Integer().deserializer())
  implicit val LongDeserializer: KDeserializer[java.lang.Long] = from(Serdes.Long().deserializer())
}
