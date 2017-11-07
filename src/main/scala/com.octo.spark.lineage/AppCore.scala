package com.octo.spark.lineage

import java.io.File

import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.util.{RDDLineageExtractor, TreeNode}

import scala.collection.mutable.ListBuffer

/**
  * Created by Y. Benabderrahmane on 11/10/2017.
  */
trait AppCore extends Serializable {

  object coreImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  val lineageFilePath = "src/main/resources/examples/lineage/lineage-data-v1.json"
  var processId: String = ""
  var spark: SparkSession = _

  def init(driverClass: Class[_], runId: Long = 0): SparkSession ={
    processId = driverClass.getCanonicalName + runId
//    if (cleanup) delete(new File(lineageFilePath))
    spark = SparkSession
      .builder()
      .appName("Spark Lineage Extractor Demo")
      .config("spark.master", "local[1]")
      .getOrCreate()
    spark
  }

  // -----------------------------------
  // RDD support
  // -----------------------------------
  def writeRddToJson(rdd: RDD[_ <: Row], outputFilePath: String, saveMode : SaveMode = SaveMode.Append): Unit = {
    lineSparkAction(rdd, outputFilePath, IoType.WRITE)
    val schema = rdd.take(1)(0).schema
    spark.createDataFrame(rdd.asInstanceOf[RDD[Row]], schema).write.mode(saveMode).partitionBy("version").json(outputFilePath)
  }
  def readJson(filePath: String, referenceDataset: Boolean = false): RDD[Row] = {
    val readRdd = spark.read.json(filePath).rdd
    lineSparkAction(readRdd, filePath, IoType.READ, referenceDataset)
    readRdd
  }
  def readPrunedJson(filePath: String, version: String, referenceDataset: Boolean = false): RDD[Row] = {
    val readRdd = spark.read.json(filePath).where(s"version == $version").rdd
    lineSparkAction(readRdd, filePath, IoType.READ, referenceDataset)
    readRdd
  }
  // -----------------------------------

  // -----------------------------------
  // DF support
  // -----------------------------------
  def readJsonDF(filePath: String): DataFrame = {
    spark.read.json(filePath)
  }
  def writeJson(df: DataFrame, path: String, lineData: Boolean = true, saveMode : SaveMode = SaveMode.Overwrite): Unit = {
    if (lineData) {
      lineSparkAction(df, path, IoType.WRITE)
      df.write.mode(saveMode).partitionBy("version").json(path)
    } else {
      df.write.mode(saveMode).json(path)
    }
  }
  // -----------------------------------

  private def lineSparkAction(rdd: RDD[_ <: Row], filePath: String, ioType: IoType.Value, referenceDataset: Boolean = false) = {
    val rddLineageTree: TreeNode[String] = RDDLineageExtractor.lineageTree(rdd)
    val datasetMetadata: LineageDatasetMetadata = getDatasetMetadata(rdd, filePath, referenceDataset)
    val processInformation: String = processId
    produceLineageMessage(rddLineageTree, datasetMetadata, processInformation, ioType)
  }

  private def lineSparkAction(df: DataFrame, filePath: String, ioType: IoType.Value) = {
    val rddLineageTree: TreeNode[String] = getLineageTree(df, filePath)
    val datasetMetadata: LineageDatasetMetadata = getDatasetMetadata(df, filePath)
    val processInformation: String = processId
    produceLineageMessage(rddLineageTree, datasetMetadata, processInformation, ioType)
  }

  private def getDatasetMetadata(rdd: RDD[_ <: Row], filePath: String, referenceDataset: Boolean = false): LineageDatasetMetadata = {
    val firstRow: Row = rdd.take(1)(0)
//    val versions = rdd.keyBy[String](row => row.getAs("version").toString).reduceByKey({case (a, _) => a}).keys
    val datasetName = datasetNameFromFile(filePath)
//    versions.map(version => LineageDatasetMetadata(datasetName, filePath, version.toString, referenceDataset)).collect()
    LineageDatasetMetadata(datasetName, filePath, firstRow.getAs("version").toString, referenceDataset)
  }

  private def getDatasetMetadata(df: DataFrame, filePath: String): LineageDatasetMetadata = {
    val firstRow: Row = df.take(1)(0)
    val datasetName = datasetNameFromFile(filePath)
    LineageDatasetMetadata(datasetName, filePath, firstRow.getAs[String]("version"))
  }

  private def datasetNameFromFile(filePath: String) = {
    FilenameUtils.removeExtension(new File(filePath).getName)
  }

  private def getLineageTree(dataFrame: DataFrame, outputPath: String): TreeNode[String] = {
    TreeNode.get[String](outputPath, None, dataFrame.inputFiles.map(inputFile => {
      TreeNode.get(inputFile, None, ListBuffer[TreeNode[String]]())
    }).to[ListBuffer])
  }

  /**
    * Lineage data structure is:
    * - dataset info:
    *   - name: name of the dataset
    *   - URI of the dataset
    *   - version: version of the dataset
    * - producer: unique information defining the generating process
    * - ioType
    * - sources
    * - sinks
    *
    * @param lineageTree
    * @param datasetMetadata
    * @param processInformation
    * @param ioType
    */
  def produceLineageMessage(lineageTree: TreeNode[String], datasetMetadata: LineageDatasetMetadata,
                                    processInformation: String, ioType: IoType.Value): Unit = {
    println("------------------------------------------")
    println(s"---- Producing $ioType lineage info for: $datasetMetadata")
    println(TreeNode.describe(lineageTree))
    println(s"---- Producer: $processInformation")
    val (sources, sinks) = ioType match {
      case IoType.WRITE =>
        println(s"---- Sink:")
        println(lineageTree.value)
        println(s"---- Sources:")
        lineageTree.leafs().foreach(leaf => println(leaf.value))
        (lineageTree.leafs().map(leaf => leaf.value).toArray[String], Array(lineageTree.value))
      case IoType.READ =>
        println(s"---- Sinks:")
        println(s"---- Sources:")
        lineageTree.leafs().foreach(leaf => println(leaf.value))
        (lineageTree.leafs().map(leaf => leaf.value).toArray[String], Array[String]())
    }
    println("------------------------------------------")
    import coreImplicits._
    val lineageDf = Seq(LineageData(
      datasetMetadata.datasetName,
      ioType.toString,
      processInformation,
      sinks.mkString(","),
      sources.mkString(","),
      datasetMetadata.uri,
      datasetMetadata.version,
      datasetMetadata.reference,
      DatasetStatus.Initial.toString
    )).toDF
    writeJson(lineageDf, lineageFilePath,
      lineData = false,
      saveMode = SaveMode.Append
    )
  }

  /**
    * Delete folders and their content recursively
    * @param file
    */
  def delete(file: File): Unit = {
    println(s"Deleting folder : $file")
    val files = file.listFiles
    if (files != null) { //some JVMs return null for empty dirs
      for (f <- files) {
        if (f.isDirectory) delete(f)
        else f.delete
      }
    }
    file.delete
  }

}

object IoType extends Enumeration {
  type IoType = Value
  val READ, WRITE = Value
}
object DatasetStatus extends Enumeration {
  type DatasetStatus = Value
  val Initial, Simulation, Production = Value
}
case class LineageData(datasetName: String, ioType: String, producer: String, sinks: String, sources: String, uri: String, version: String, reference: Boolean = false, status: String) extends Serializable
case class LineageDatasetMetadata(datasetName: String, uri: String, version: String, reference: Boolean = false)