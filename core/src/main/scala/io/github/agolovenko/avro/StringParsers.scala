package io.github.agolovenko.avro

import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Base64

object StringParsers {
  val primitiveParsers: PartialFunction[(String, Schema, Path), Any] = {
    case (value, schema, _) if schema.getType == INT && schema.getLogicalType == null     => value.toInt
    case (value, schema, _) if schema.getType == LONG && schema.getLogicalType == null    => value.toLong
    case (value, schema, _) if schema.getType == FLOAT && schema.getLogicalType == null   => value.toFloat
    case (value, schema, _) if schema.getType == DOUBLE && schema.getLogicalType == null  => value.toDouble
    case (value, schema, _) if schema.getType == BOOLEAN && schema.getLogicalType == null => value.toBoolean
  }

  val base64Parsers: PartialFunction[(String, Schema, Path), Any] = {
    case (value, schema, _) if schema.getType == BYTES && schema.getLogicalType == null => ByteBuffer.wrap(parseBase64(value))
    case (value, schema, _) if schema.getType == FIXED && schema.getLogicalType == null => parseBase64(value)
  }

  def dateParser(formatter: DateTimeFormatter): PartialFunction[(String, Schema, Path), Int] = {
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.date() => LocalDate.parse(value, formatter).toEpochDay.toInt
  }

  def timeParsers(formatter: DateTimeFormatter): PartialFunction[(String, Schema, Path), Any] = {
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timeMillis() => LocalTime.parse(value, formatter).get(ChronoField.MILLI_OF_DAY)
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timeMicros() => LocalTime.parse(value, formatter).getLong(ChronoField.MICRO_OF_DAY)
  }

  def zonedDateTimeParsers(formatter: DateTimeFormatter): PartialFunction[(String, Schema, Path), Long] = {
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timestampMillis() => ZonedDateTime.parse(value, formatter).toInstant.toEpochMilli
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timestampMicros() => toEpochMicros(ZonedDateTime.parse(value, formatter).toInstant)
  }

  def localDateTimeParsers(formatter: DateTimeFormatter, zoneId: ZoneId): PartialFunction[(String, Schema, Path), Long] = {
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timestampMillis() =>
      LocalDateTime.parse(value, formatter).atZone(zoneId).toInstant.toEpochMilli
    case (value, schema, _) if schema.getLogicalType == LogicalTypes.timestampMicros() =>
      toEpochMicros(LocalDateTime.parse(value, formatter).atZone(zoneId).toInstant)
  }

  private def parseBase64(str: String): Array[Byte] = Base64.getDecoder.decode(str)

  private def toEpochMicros(ins: Instant): Long = ins.getEpochSecond * 1000000L + ins.getNano / 1000L
}
