package io.github.agolovenko.avro

import org.apache.avro.generic.GenericData
import org.apache.avro.{LogicalTypes, Schema}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.{LocalDate, ZoneId}
import java.util
import scala.jdk.CollectionConverters._
import scala.util.Random

class RandomData(
    rootSchema: Schema,
    total: Int,
    typedGenerators: Map[String, Random => Any] = Map.empty,
    namedGenerators: Map[Path, Random => Any] = Map.empty,
    seed: Long = System.currentTimeMillis,
    maxLength: Int = 1 << 4
) extends Iterator[Any] {
  import Schema.Type._

  private var count        = 0
  private val random       = new Random(seed)
  private val path         = new Path
  private val namedGensMap = namedGenerators.map { case (path, gen) => path.mkString(withArrayIdx = false) -> gen }

  override def hasNext: Boolean = count < total

  override def next(): Any = {
    count += 1
    path.clear()
    generate(rootSchema)
  }

  private def namedGenerator                 = if (namedGensMap.nonEmpty) namedGensMap.get(path.mkString(withArrayIdx = false)) else None
  private def typedGenerator(schema: Schema) = if (typedGenerators.nonEmpty) typedGenerators.get(typeName(schema)) else None

  private def generate(schema: Schema): Any =
    namedGenerator
      .orElse(typedGenerator(schema))
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
      } { generator =>
        val result = generator(random)

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

  def dateGenerator(fromDate: LocalDate, maxDays: Int): Map[String, Random => Any] = Map(
    LogicalTypes.date().getName -> (implicit random => randomDay(fromDate, maxDays))
  )

  val timeGenerators: Map[String, Random => Any] = Map(
    LogicalTypes.timeMillis().getName -> (implicit random => randomMillisOfDay),
    LogicalTypes.timeMicros().getName -> (implicit random => randomMicrosOfDay)
  )

  def dateTimeGenerators(fromDate: LocalDate, maxDays: Int, zoneId: ZoneId): Map[String, Random => Any] = Map(
    LogicalTypes.timestampMillis().getName -> (implicit random => randomDayEpochSecond(fromDate, maxDays, zoneId) * 1000L + randomMillisOfDay),
    LogicalTypes.timestampMicros().getName -> (implicit random => randomDayEpochSecond(fromDate, maxDays, zoneId) * 1000000L + randomMicrosOfDay)
  )
}
