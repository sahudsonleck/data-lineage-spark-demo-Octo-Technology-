package org.apache.spark.util

import org.scalatest._

import scala.collection.mutable.ListBuffer

/**
  * Created by Y. Benabderrahmane on 09/10/2017.
  */
class TreeNodeSpec extends WordSpec {
  "TreeNode" should {
    "describe correctly the constructed tree" in {
      val tree = TreeNode[String]("P", None, ListBuffer(
        TreeNode[String]("C1", None, ListBuffer(
          TreeNode[String]("Leaf 1", None),
          TreeNode[String]("C3", None, ListBuffer(
            TreeNode[String]("Leaf 2", None),
            TreeNode[String]("Leaf 3", None)
            )),
          TreeNode[String]("Leaf 8", None)
        ))
      ))
      println(tree)
      assert("Node [P, ListBuffer(Node [C1, ListBuffer(Node [Leaf 1, ListBuffer()], Node [C3, ListBuffer(Node [Leaf 2, ListBuffer()], Node [Leaf 3, ListBuffer()])], Node [Leaf 8, ListBuffer()])])]" equals  tree.toString)
      println(TreeNode.describe(tree))
    }
    "manage correctly parents" in {
      val tree = TreeNode.get[String]("P", None, ListBuffer(
        TreeNode.get[String]("C1", None, ListBuffer(
          TreeNode[String]("Leaf 1", None),
          TreeNode.get[String]("C3", None, ListBuffer(
            TreeNode[String]("Leaf 2", None),
            TreeNode[String]("Leaf 3", None)
          )),
          TreeNode[String]("Leaf 8", None)
        ))
      ))
      println(tree)
      assert("Node [P, ListBuffer(Node [C1, ListBuffer(Node [Leaf 1, ListBuffer()], Node [C3, ListBuffer(Node [Leaf 2, ListBuffer()], Node [Leaf 3, ListBuffer()])], Node [Leaf 8, ListBuffer()])])]" equals  tree.toString)
      println(TreeNode.describe(tree, full = true))
    }
    "return the correct leafs" in {
      val tree = TreeNode.get[String]("P", None, ListBuffer(
        TreeNode.get[String]("C1", None, ListBuffer(
          TreeNode[String]("Leaf 1", None),
          TreeNode.get[String]("C3", None, ListBuffer(
            TreeNode[String]("Leaf 2", None),
            TreeNode[String]("Leaf 3", None)
          )),
          TreeNode[String]("Leaf 8", None)
        ))
      ))
      val leafs = tree.leafs()
      println(s"---Leafs: $leafs")
      assert("ListBuffer(Node [Leaf 1, ListBuffer()], Node [Leaf 2, ListBuffer()], Node [Leaf 3, ListBuffer()], Node [Leaf 8, ListBuffer()])" equals leafs.toString)
    }
  }

}
