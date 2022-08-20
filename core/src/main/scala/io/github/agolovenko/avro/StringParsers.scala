package io.github.agolovenko.avro

import org.apache.avro.Schema.Type._
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Base64

case class ParserContext(value: String, schema: Schema, path: Path)

object StringParsers {
  val primitiveParsers: PartialFunction[ParserContext, Any] = {
    case ctx if ctx.schema.getType == INT && ctx.schema.getLogicalType == null     => ctx.value.toInt
    case ctx if ctx.schema.getType == LONG && ctx.schema.getLogicalType == null    => ctx.value.toLong
    case ctx if ctx.schema.getType == FLOAT && ctx.schema.getLogicalType == null   => ctx.value.toFloat
    case ctx if ctx.schema.getType == DOUBLE && ctx.schema.getLogicalType == null  => ctx.value.toDouble
    case ctx if ctx.schema.getType == BOOLEAN && ctx.schema.getLogicalType == null => ctx.value.toBoolean
  }

  val base64Parsers: PartialFunction[ParserContext, Any] = {
    case ctx if ctx.schema.getType == BYTES && ctx.schema.getLogicalType == null => ByteBuffer.wrap(parseBase64(ctx.value))
    case ctx if ctx.schema.getType == FIXED && ctx.schema.getLogicalType == null => parseBase64(ctx.value)
  }

  def dateParser(formatter: DateTimeFormatter): PartialFunction[ParserContext, Int] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.date() => LocalDate.parse(ctx.value, formatter).toEpochDay.toInt
  }

  def timeParsers(formatter: DateTimeFormatter): PartialFunction[ParserContext, Any] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timeMillis() => LocalTime.parse(ctx.value, formatter).get(ChronoField.MILLI_OF_DAY)
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timeMicros() => LocalTime.parse(ctx.value, formatter).getLong(ChronoField.MICRO_OF_DAY)
  }

  def zonedDateTimeParsers(formatter: DateTimeFormatter): PartialFunction[ParserContext, Long] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMillis() => ZonedDateTime.parse(ctx.value, formatter).toInstant.toEpochMilli
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMicros() => toEpochMicros(ZonedDateTime.parse(ctx.value, formatter).toInstant)
  }

  def localDateTimeParsers(formatter: DateTimeFormatter, zoneId: ZoneId): PartialFunction[ParserContext, Long] = {
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMillis() =>
      LocalDateTime.parse(ctx.value, formatter).atZone(zoneId).toInstant.toEpochMilli
    case ctx if ctx.schema.getLogicalType == LogicalTypes.timestampMicros() =>
      toEpochMicros(LocalDateTime.parse(ctx.value, formatter).atZone(zoneId).toInstant)
  }

  private def parseBase64(str: String): Array[Byte] = Base64.getDecoder.decode(str)

  private def toEpochMicros(ins: Instant): Long = ins.getEpochSecond * 1000000L + ins.getNano / 1000L
}
