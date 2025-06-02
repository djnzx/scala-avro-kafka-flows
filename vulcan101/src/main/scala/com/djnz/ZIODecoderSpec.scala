package com.djnz

import com.djnz.domain.c
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.scalatest.Inside
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import vulcan.Codec
import zio._
import zio.kafka.serde.Deserializer
import zio.kafka.serde.Serde
import zio.kafka.serde.Serializer

import java.io.ByteArrayOutputStream

object domain {
  import vulcan.generic._
  case class Person(a: String, b: Int)
  val c = Codec.derive[Person]
  val s = c.schema.getOrElse(???)
  pprint.log(s.toString(true))

  val hh: Headers = new RecordHeaders()
  val schemaRegistryClient: SchemaRegistryClient = new MockSchemaRegistryClient()

  def ddd(bytes: Array[Byte]) = new KafkaAvroDeserializer(schemaRegistryClient).deserialize("topic1",hh, bytes)

  def des: Deserializer[Any, Person] =
    (topic: String, headers: Headers, data: Array[Byte]) =>
      ZIO.attempt(
        c.schema match {
          case Left(e)       => throw new RuntimeException("can't create a schema")
          case Right(schema) =>
            val obj = ddd(data)
            c.decode(obj, schema) match {
              case Left(e)  => throw new RuntimeException(s"decode error $e")
              case Right(a) => a
            }
        }
      )

  def ser: Serializer[Any, Person] = (topic: String, headers: Headers, value: Person) =>
    ZIO.attempt(
      c.schema match {
        case Left(e)       => throw new RuntimeException("can't create a schema")
        case Right(schema) =>
          c.encode(value) match {
            case Left(e)      => throw new RuntimeException("Encode error")
            case Right(value) =>
              val t: c.AvroType = value
              pprint.log(value)
              val os = new ByteArrayOutputStream()
              val enc = EncoderFactory.get.binaryEncoder(os, null)
              val dw = new GenericDatumWriter[GenericData.Record](s)
              dw.write(t.asInstanceOf[GenericData.Record], enc)
              // flush encoder
              enc.flush()
              // close stream
              os.close()
              // original data
              os.toByteArray
          }
      }
    )





}

class ZIODecoderSpec extends AnyFunSuite with Matchers with ScalaCheckPropertyChecks with Inside with Tools {

  test("1") {
    import domain._

  }

}
