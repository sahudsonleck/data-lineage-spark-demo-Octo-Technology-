package com.octo.spark.lineage

import java.io.File

import org.apache.spark.sql.Encoders
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import scala.collection.JavaConverters._

/**
  * Created by Y. Benabderrahmane on 12/10/2017.
  */
object LineageGraphGenerator extends AppCore {
  val lineageGraphDbPath = "src/main/resources/examples/lineage-graph"
  case object ProducedUsing extends RelationshipType {
    override def name(): String = "ProducedUsing"
  }

  def main(args: Array[String]): Unit = {
    init(getClass)

    val lineageDf = readJsonDF(lineageFilePath)
    lineageDf.show(false)
    lineageDf.printSchema()

    val graphDbFactory = new GraphDatabaseFactory
    val graphDb : GraphDatabaseService = graphDbFactory.newEmbeddedDatabase(new File(lineageGraphDbPath))

    implicit val personEncoder = Encoders.product[LineageData]
    val lineageDataArr = lineageDf.as[LineageData].collect()
    println(lineageDataArr.toList)

    try {
      val trsx = graphDb.beginTx
      try {
        // Concatenate uri and version to get unique dataset ids
        lineageDataArr.foreach(ld => {
          IoType.withName(ld.ioType) match {
            case IoType.WRITE =>
              ld.sources.split(",").foreach(source => {
                val node = getOrCreateNode(graphDb, ld, "lineage-data-all")
                node.addLabel(Label.label("lineage-data-out"))
                node.setProperty("source", "")
                addSink(node, Seq(ld.producer, source).mkString("-"))
              })
            case IoType.READ =>
              val node = getOrCreateNode(graphDb, ld, "lineage-data-all")
              node.addLabel(Label.label("lineage-data-in"))
              addSource(node, Seq(ld.producer, ld.sources).mkString("-"))
              node.setProperty("sink", Array[String]())
          }
        })
        trsx.success()
      } catch {
        case e: Throwable => e.printStackTrace()
      } finally {
        trsx.close()
      }

      val trs = graphDb.beginTx
      try {
        val result = graphDb.execute("MATCH (n) RETURN n")
        println(result.resultAsString())

        val graphIterator = graphDb.getAllNodes.iterator()
        while (graphIterator.hasNext) {
          val node = graphIterator.next()
          println(s"Checking for node: ${node.getId}...")
          val nodeSinks = node.getProperty("sink").asInstanceOf[Array[String]]
          nodeSinks.foreach(nodeSink => {
            val sourceNodes2 = graphDb.findNodes(Label.label("lineage-data-in"))
            while(sourceNodes2.hasNext) {
              val sourceNode = sourceNodes2.next
              if (sourceNode.getProperty("source").asInstanceOf[Array[String]].contains(nodeSink)) {
                println(s"  Evaluating source node : ${sourceNode.getId}")
                if ((sourceNode.getProperty("reference").toString.equals("true")
                  && sourceNode.getProperty("version").equals(node.getProperty("version")))
                  || sourceNode.getProperty("reference").toString.equals("false")
                ) {
                  println(s"    Linking sink node: ${node.getId} to source node: ${sourceNode.getId}, source: ${sourceNode.getProperty("source")}")
                  val rel = node.createRelationshipTo(sourceNode, ProducedUsing)
                  rel.setProperty("producer", node.getProperty("producer"))
                }
              }
            }
          })
        }

        trs.success()
      } finally {
        trs.close()
      }

      println("====================================================")

      val trs2 = graphDb.beginTx
      try {
        val result = graphDb.execute("MATCH (node)<-[:ProducedUsing]-(node2) RETURN node")
        println(result.resultAsString())

        val result2 = graphDb.execute("MATCH (node)-[:ProducedUsing]->(node2) RETURN node")
        println(result2.resultAsString())

        val relIt = graphDb.getAllRelationships.iterator()
        while (relIt.hasNext) {
          val rel = relIt.next()
          println(s"---> Dataset: [${rel.getStartNode.getProperty("datasetName")}] has been produced by [${rel.getProperty("producer")}] using [${rel.getEndNode.getProperty("datasetName")}]")
        }

        println()

        val nodesIt = graphDb.getAllNodes.iterator()
        while (nodesIt.hasNext) {
          val node = nodesIt.next()
          if (node.hasRelationship(Direction.OUTGOING)) {
            println(s"Node [${node.getId}]---> [Dataset: ${node.getProperty("datasetName")}, Version: ${node.getProperty("version")}] has been produced by:")
            for (rel: Relationship <- node.getRelationships(Direction.OUTGOING).asScala) {
                println(s" [${rel.getProperty("producer")}] using: [Dataset: ${rel.getEndNode.getProperty("datasetName")}, Version: ${rel.getEndNode.getProperty("version")}]")
            }
          }
        }

        trs2.success()
      } finally {
        trs2.close()
      }
    } finally {
      // Remove this, just to force it to run for now
      lineageDf.count()

      graphDb.shutdown()

      // Cleanup
      delete(new File(lineageGraphDbPath))
    }
  }

  def getOrCreateNode(graphDb: GraphDatabaseService, ld: LineageData, label: String): Node = {
    val realLabel = Label.label(label)
    val uniqueId = Seq(ld.uri, ld.version).mkString("-")
    var node = graphDb.findNode(realLabel, "uniqueId", uniqueId)
    if (node == null || !node.getProperty("ioType").equals(ld.ioType)) {
      node = graphDb.createNode(realLabel)
      node.setProperty("datasetName", ld.datasetName)
      node.setProperty("ioType", ld.ioType)
      node.setProperty("producer", ld.producer)
      node.setProperty("uri", ld.uri)
      node.setProperty("version", ld.version)
      node.setProperty("reference", ld.reference)
      node.setProperty("uniqueId", uniqueId)
      println(s"Created node: $node")
    } else {
      println(s"Found node: $node")
    }
    node
  }

  def addSink(node: Node, sink: String): Unit = {
    if (node.hasProperty("sink")) {
      val sinkProp = node.getProperty("sink").asInstanceOf[Array[String]]
      val newSinkProp = sinkProp :+ sink
      println(s"Updating sink with: [${newSinkProp.toList}] for node: [${node.getId}]")
      node.setProperty("sink", newSinkProp)
    } else {
      println(s"Creating sink: [$sink] for node: [${node.getId}]")
      node.setProperty("sink", Array(sink))
    }
  }

  def addSource(node: Node, source: String): Unit = {
    if (node.hasProperty("source")) {
      val sourceProp = node.getProperty("source").asInstanceOf[Array[String]]
      val newSourceProp = sourceProp :+ source
      println(s"Updating source with: [${newSourceProp.toList}] for node: [${node.getId}]")
      node.setProperty("source", newSourceProp)
    } else {
      println(s"Creating source: [$source] for node: [${node.getId}]")
      node.setProperty("source", Array(source))
    }
  }

}