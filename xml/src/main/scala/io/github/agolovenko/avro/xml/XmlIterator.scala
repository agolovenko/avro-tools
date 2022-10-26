package io.github.agolovenko.avro.xml

import io.github.agolovenko.avro.closeOnError

import java.io.{Reader, StringWriter}
import javax.xml.stream.XMLInputFactory
import scala.util.control.NonFatal
import scala.xml.{Elem, XML}

class XmlIterator(elementName: Option[String])(reader: Reader) extends Iterator[Elem] with AutoCloseable {
  private val xmlReader = XMLInputFactory.newInstance().createXMLEventReader(reader)

  private def isStart: Boolean = closeOnError(this) {
    try {
      val evt = xmlReader.peek()
      evt.isStartElement && elementName.forall(_ == evt.asStartElement().getName.getLocalPart)
    } catch { case NonFatal(ex) => throw new XmlException("Failed to advance through xml", ex) }
  }

  override def hasNext: Boolean = closeOnError(this) {
    while (xmlReader.hasNext && !isStart) {
      xmlReader.nextEvent()
    }

    if (xmlReader.hasNext) true
    else {
      close()
      false
    }
  }

  override def next(): Elem = closeOnError(this) {
    try {
      val out       = new StringWriter()
      var openCount = 0

      do {
        val evt = xmlReader.nextEvent()
        evt.writeAsEncodedUnicode(out)

        openCount += (if (evt.isStartElement) 1 else if (evt.isEndElement) -1 else 0)
      } while (openCount > 0)

      XML.loadString(out.toString)
    } catch { case NonFatal(ex) => throw new XmlException("Failed to serialize element", ex) }
  }

  override def close(): Unit = {
    xmlReader.close()
    reader.close()
  }
}
