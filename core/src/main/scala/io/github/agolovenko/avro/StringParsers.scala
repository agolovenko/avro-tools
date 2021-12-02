package io.github.agolovenko.avro

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema.Type._

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Base64

object StringParsers {
  val primitiveParsers: Map[String, String => Any] = Map(
    INT.name()     -> (_.toInt),
    LONG.name()    -> (_.toLong),
    FLOAT.name()   -> (_.toFloat),
    DOUBLE.name()  -> (_.toDouble),
    BOOLEAN.name() -> (_.toBoolean)
  )

  val base64Parsers: Map[String, String => Any] = Map(
    BYTES.name() -> (str => ByteBuffer.wrap(parseBase64(str))),
    FIXED.name() -> parseBase64
  )

  def dateParser(formatter: DateTimeFormatter): Map[String, String => Any] = Map(
    LogicalTypes.date().getName -> (LocalDate.parse(_, formatter).toEpochDay.toInt)
  )

  def timeParsers(formatter: DateTimeFormatter): Map[String, String => Any] = Map(
    LogicalTypes.timeMillis().getName -> (LocalTime.parse(_, formatter).get(ChronoField.MILLI_OF_DAY)),
    LogicalTypes.timeMicros().getName -> (LocalTime.parse(_, formatter).getLong(ChronoField.MICRO_OF_DAY))
  )

  def zonedDateTimeParsers(formatter: DateTimeFormatter): Map[String, String => Any] = Map(
    LogicalTypes.timestampMillis().getName -> (ZonedDateTime.parse(_, formatter).toInstant.toEpochMilli),
    LogicalTypes.timestampMicros().getName -> (s => toEpochMicros(ZonedDateTime.parse(s, formatter).toInstant))
  )

  def localDateTimeParsers(formatter: DateTimeFormatter, zoneId: ZoneId): Map[String, String => Any] = Map(
    LogicalTypes.timestampMillis().getName -> (LocalDateTime.parse(_, formatter).atZone(zoneId).toInstant.toEpochMilli),
    LogicalTypes.timestampMicros().getName -> (s => toEpochMicros(LocalDateTime.parse(s, formatter).atZone(zoneId).toInstant))
  )

  private def parseBase64(str: String) = Base64.getDecoder.decode(str)

  private def toEpochMicros(ins: Instant) = ins.getEpochSecond * 1000000L + ins.getNano / 1000L
}
