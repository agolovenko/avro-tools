package io.github.agolovenko.avro

import org.apache.avro.Schema.Type.{BYTES, FIXED}
import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter

case class EncoderContext(value: Any, schema: Schema, path: Path)

object StringEncoders {
  val base64Encoders: PartialFunction[EncoderContext, String] = {
    case ctx if ctx.schema.getType == BYTES => toBase64(ctx.value.asInstanceOf[ByteBuffer].array())
    case ctx if ctx.schema.getType == FIXED => toBase64(ctx.value.asInstanceOf[GenericData.Fixed].bytes())
  }

  def dateEncoder(formatter: DateTimeFormatter): PartialFunction[EncoderContext, String] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.date() => LocalDate.ofEpochDay(ctx.value.asInstanceOf[Int].toLong).format(formatter)
  }

  def timeEncoders(formatter: DateTimeFormatter): PartialFunction[EncoderContext, String] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timeMillis() =>
      LocalTime.ofNanoOfDay(ctx.value.asInstanceOf[Int] * 1000000L).format(formatter)
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timeMicros() => LocalTime.ofNanoOfDay(ctx.value.asInstanceOf[Long] * 1000L).format(formatter)
  }

  def dateTimeEncoders(formatter: DateTimeFormatter, zoneId: ZoneId): PartialFunction[EncoderContext, String] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMillis() =>
      val ins = Instant.ofEpochMilli(ctx.value.asInstanceOf[Long])
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMicros() =>
      val ins = Instant.ofEpochSecond(ctx.value.asInstanceOf[Long] / 1000000L, (ctx.value.asInstanceOf[Long] % 1000000L) * 1000L)
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
  }
}
