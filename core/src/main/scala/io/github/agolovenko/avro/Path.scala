package io.github.agolovenko.avro

import io.github.agolovenko.avro.PathEntry.FieldEntry
import io.github.agolovenko.avro.StackType.Stack

class Path private (private val pathStack: Stack[PathEntry]) {
  def push(entry: PathEntry): Unit = pathStack.push(entry)
  def pop(): PathEntry             = pathStack.pop()
  def clear(): Unit                = pathStack.clear()
  def peek: PathEntry              = pathStack.top

  def mkString(fieldsOnly: Boolean): String = {
    val it = if (fieldsOnly) pathStack.reverseIterator.filter(_.isField) else pathStack.reverseIterator

    it.map(_.toString).mkString("/", "/", "")
  }

  def =~=(other: Path): Boolean =
    pathStack.reverseIterator.filter(_.isField) sameElements other.pathStack.reverseIterator.filter(_.isField)

  override def toString: String = mkString(fieldsOnly = false)

  override def hashCode(): Int = pathStack.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case other: Path => pathStack.reverseIterator sameElements other.pathStack.reverseIterator
    case _           => false
  }

  override def clone(): Path = new Path(pathStack.clone())
}

object Path {
  def empty: Path = new Path(new Stack[PathEntry]())

  def apply(entries: String*): Path = {
    val path = Path.empty
    entries.foreach { field => path.push(FieldEntry(field)) }
    path
  }
}
