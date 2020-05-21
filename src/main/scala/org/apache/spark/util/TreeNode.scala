package org.apache.spark.util

import scala.collection.mutable.ListBuffer

/**
  * Created by Y. Benabderrahmane on 09/10/2017.
  */
case class TreeNode[T](value: T, var parent: Option[TreeNode[T]], children: ListBuffer[TreeNode[T]] = ListBuffer[TreeNode[T]]()) {
  def isLeaf: Boolean = children.isEmpty
  def leafs(isRoot: Boolean = true): ListBuffer[TreeNode[T]] = {
    if (isLeaf) {
      if (isRoot) ListBuffer[TreeNode[T]]() else ListBuffer[TreeNode[T]](this)
    } else {
      children.map(ch => ch.leafs(isRoot = false)).foldLeft(ListBuffer[TreeNode[T]]())((p, n) => p ++ n)
    }
  }
  def describe: String = s"Node [$value, ${children.map{ child => child.describe}}]"
  override def toString: String = describe
}

object TreeNode {
  def get[T](value: T, parent: Option[TreeNode[T]], children: ListBuffer[TreeNode[T]] = ListBuffer[TreeNode[T]]()): TreeNode[T] = {
    val node: TreeNode[T] = TreeNode[T](value, parent, children)
    children.foreach(ch => ch.parent = Option(node))
    node
  }

  def describe[T](tree: TreeNode[T], level: Int = 0, full: Boolean = false ): String = {
    val childrenString = if (!tree.isLeaf) {
      val padding: String = Array.range(0, level).foldLeft(" ")((p, n) => s"$p  ")
      val str: String = tree.children.map(c => s"\n$padding${describe(c, level + 1, full)}").foldLeft("")((p, n) => s"$p$n")
      str
    } else {
      ""
    }
    val parentDesc = if (full) s" , [parent: ${if (tree.parent.isDefined) tree.parent.get.value else ""}]" else ""
    s"${tree.value}$parentDesc$childrenString"
  }
}