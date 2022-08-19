package io.github.agolovenko.avro

import org.apache.avro.Schema.Type.{BYTES, FIXED}
import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter

object StringEncoders {
  val base64Encoders: PartialFunction[(Any, Schema, Path), String] = {
    case (bytes, schema, _) if schema.getType == BYTES => toBase64(bytes.asInstanceOf[ByteBuffer].array())
    case (bytes, schema, _) if schema.getType == FIXED => toBase64(bytes.asInstanceOf[GenericData.Fixed].bytes())
  }

  def dateEncoder(formatter: DateTimeFormatter): PartialFunction[(Any, Schema, Path), String] = {
    case (epochDays, schema, _) if schema.getLogicalType == LogicalTypes.date() => LocalDate.ofEpochDay(epochDays.asInstanceOf[Int].toLong).format(formatter)
  }

  def timeEncoders(formatter: DateTimeFormatter): PartialFunction[(Any, Schema, Path), String] = {
    case (millis, schema, _) if schema.getLogicalType == LogicalTypes.timeMillis() =>
      LocalTime.ofNanoOfDay(millis.asInstanceOf[Int] * 1000000L).format(formatter)
    case (micros, schema, _) if schema.getLogicalType == LogicalTypes.timeMicros() => LocalTime.ofNanoOfDay(micros.asInstanceOf[Long] * 1000L).format(formatter)
  }

  def dateTimeEncoders(formatter: DateTimeFormatter, zoneId: ZoneId): PartialFunction[(Any, Schema, Path), String] = {
    case (millis, schema, _) if schema.getLogicalType == LogicalTypes.timestampMillis() =>
      val ins = Instant.ofEpochMilli(millis.asInstanceOf[Long])
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
    case (micros, schema, _) if schema.getLogicalType == LogicalTypes.timestampMicros() =>
      val ins = Instant.ofEpochSecond(micros.asInstanceOf[Long] / 1000000L, (micros.asInstanceOf[Long] % 1000000L) * 1000L)
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
  }
}
