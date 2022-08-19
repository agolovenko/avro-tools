package io.github.agolovenko.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, ZoneId}
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Random

class RandomData(
    rootSchema: Schema,
    total: Int,
    generators: PartialFunction[(Schema, Path, Random), Any],
    seed: Long = System.currentTimeMillis,
    maxLength: Int = 1 << 4
) extends Iterator[Any] {
  import Schema.Type._

  private var count            = 0
  private val random           = new Random(seed)
  private val path             = new Path
  private val liftedGenerators = generators.lift

  override def hasNext: Boolean = count < total

  override def next(): Any = {
    count += 1
    path.clear()
    generate(rootSchema)
  }

  private def generate(schema: Schema): Any =
    liftedGenerators((schema, path, random))
      .fold {
        schema.getType match {
          case RECORD =>
            val record = new GenericData.Record(schema)

            schema.getFields.asScala.foreach { field =>
              path.push(field.name)
              record.put(field.name, generate(field.schema))
              path.pop()
            }
            record
          case ENUM =>
            val symbols = schema.getEnumSymbols
            new GenericData.EnumSymbol(schema, symbols.get(random.nextInt(symbols.size)))
          case ARRAY =>
            val length = random.nextInt(maxLength)
            val array  = new GenericData.Array[Any](length, schema)
            for (_ <- 0 until length) {
              array.add(generate(schema.getElementType))
            }
            array
          case MAP =>
            val length = random.nextInt(maxLength)
            val map    = new util.HashMap[String, Any](length)
            for (_ <- 0 until length) {
              map.put(randomString(random), generate(schema.getValueType))
            }
            map
          case UNION =>
            val types = schema.getTypes
            generate(types.get(random.nextInt(types.size)))
          case FIXED =>
            val bytes = new Array[Byte](schema.getFixedSize)
            random.nextBytes(bytes)
            new GenericData.Fixed(schema, bytes)
          case BYTES =>
            val bytes = new Array[Byte](maxLength)
            random.nextBytes(bytes)
            ByteBuffer.wrap(bytes)
          case STRING  => randomString(random)
          case INT     => random.nextInt()
          case LONG    => random.nextLong()
          case FLOAT   => random.nextFloat()
          case DOUBLE  => random.nextDouble()
          case BOOLEAN => random.nextBoolean()
          case NULL    => null
        }
      } { result =>
        if (!GenericData.get().validate(schema, result))
          throw new IllegalArgumentException(s"Generated value $result isn't of type ${typeName(schema)} @ $path")

        result
      }

  private def randomString(random: Random): String = {
    val length = random.nextInt(maxLength)
    val bytes  = new Array[Byte](length)
    for (i <- 0 until length) {
      bytes(i) = ('a' + random.nextInt('z' - 'a')).toByte
    }

    new String(bytes, StandardCharsets.UTF_8)
  }
}

object RandomData {
  def randomDay(fromDate: LocalDate, maxDays: Int)(implicit random: Random): Int = fromDate.toEpochDay.intValue() + random.nextInt(maxDays)

  def randomDayEpochSecond(fromDate: LocalDate, maxDays: Int, zoneId: ZoneId)(implicit random: Random): Long =
    LocalDate.ofEpochDay(randomDay(fromDate, maxDays).toLong).atStartOfDay().atZone(zoneId).toEpochSecond

  def randomMillisOfDay(implicit random: Random): Int  = random.nextInt(24 * 3600 * 1000)
  def randomMicrosOfDay(implicit random: Random): Long = randomMillisOfDay(random).toLong * random.nextInt(1000)

  val uuidGenerator: PartialFunction[(Schema, Path, Random), String] = {
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.uuid() =>
      val mostSigBits  = (random.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L
      val leastSigBits = (random.nextLong() | 0x8000000000000000L) & 0xBFFFFFFFFFFFFFFFL

      new UUID(mostSigBits, leastSigBits).toString
  }

  def dateGenerator(fromDate: LocalDate, maxDays: Int): PartialFunction[(Schema, Path, Random), Int] = {
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.date() => randomDay(fromDate, maxDays)(random)
  }

  val timeGenerators: PartialFunction[(Schema, Path, Random), AnyVal] = {
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.timeMillis() => randomMillisOfDay(random)
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.timeMicros() => randomMicrosOfDay(random)
  }

  def dateTimeGenerators(fromDate: LocalDate, maxDays: Int, zoneId: ZoneId): PartialFunction[(Schema, Path, Random), Long] = {
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.timestampMillis() =>
      randomDayEpochSecond(fromDate, maxDays, zoneId)(random) * 1000L + randomMillisOfDay(random)
    case (schema, _, random) if schema.getLogicalType == LogicalTypes.timestampMicros() =>
      randomDayEpochSecond(fromDate, maxDays, zoneId)(random) * 1000000L + randomMicrosOfDay(random)
  }
}
