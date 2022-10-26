package io.github.agolovenko.avro.xml

import scala.util.control.NoStackTrace

class XmlException(message: String, cause: Throwable = null) extends RuntimeException(message, cause) with NoStackTrace
