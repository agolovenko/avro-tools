package io.github.agolovenko.avro

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema.Type.{BYTES, FIXED}
import org.apache.avro.generic.GenericData

import java.nio.ByteBuffer
import java.time._
import java.time.format.DateTimeFormatter

object StringEncoders {
  val base64Encoders: Map[String, Any => String] = Map(
    BYTES.name() -> (bytes => toBase64(bytes.asInstanceOf[ByteBuffer].array())),
    FIXED.name() -> (bytes => toBase64(bytes.asInstanceOf[GenericData.Fixed].bytes()))
  )

  def dateEncoder(formatter: DateTimeFormatter): Map[String, Any => String] = Map(
    LogicalTypes.date().getName -> (days => LocalDate.ofEpochDay(days.asInstanceOf[Int].toLong).format(formatter))
  )

  def timeEncoders(formatter: DateTimeFormatter): Map[String, Any => String] = Map(
    LogicalTypes.timeMillis().getName -> (millis => LocalTime.ofNanoOfDay(millis.asInstanceOf[Int] * 1000000L).format(formatter)),
    LogicalTypes.timeMicros().getName -> (micros => LocalTime.ofNanoOfDay(micros.asInstanceOf[Long] * 1000L).format(formatter))
  )

  def dateTimeEncoders(formatter: DateTimeFormatter, zoneId: ZoneId): Map[String, Any => String] = Map(
    LogicalTypes.timestampMillis().getName -> (millis => {
      val ins = Instant.ofEpochMilli(millis.asInstanceOf[Long])
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
    }),
    LogicalTypes.timestampMicros().getName -> (micros => {
      val ins = Instant.ofEpochSecond(micros.asInstanceOf[Long] / 1000000L, (micros.asInstanceOf[Long] % 1000000L) * 1000L)
      val dt  = ZonedDateTime.ofInstant(ins, zoneId)
      dt.format(formatter)
    })
  )
}
