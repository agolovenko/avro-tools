package io.github.agolovenko.avro

import io.github.agolovenko.avro.StackType.Stack

class Path {
  private val pathStack = new Stack[String]()

  def push(fieldName: String): Unit = pathStack.push(fieldName)
  def push(arrayIdx: Int): Unit     = pathStack.push(s"[$arrayIdx]")
  def pop(): String                 = pathStack.pop()
  def clear(): Unit                 = pathStack.clear()
  def peek: String                  = pathStack.top

  def mkString(withArrayIdx: Boolean): String = {
    val it = if (withArrayIdx) pathStack.reverseIterator else pathStack.reverseIterator.filterNot(_.startsWith("["))

    it.mkString("/", "/", "")
  }

  def =~=(other: Path): Boolean =
    pathStack.reverseIterator.filterNot(_.startsWith("[")) sameElements other.pathStack.reverseIterator.filterNot(_.startsWith("["))

  override def toString: String = mkString(withArrayIdx = true)

  override def hashCode(): Int = pathStack.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case other: Path => pathStack.reverseIterator sameElements other.pathStack.reverseIterator
    case _           => false
  }
}

object Path {
  def apply(elements: String*): Path = {
    val path = new Path
    elements.foreach(path.push)
    path
  }
}
