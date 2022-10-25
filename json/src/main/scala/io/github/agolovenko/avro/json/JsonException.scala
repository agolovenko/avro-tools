package io.github.agolovenko.avro.json

import scala.util.control.NoStackTrace

class JsonException(message: String, cause: Throwable = null) extends RuntimeException(message, cause) with NoStackTrace
