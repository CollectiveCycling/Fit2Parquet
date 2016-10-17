package com.collectivecycling.parquet

import java.io.File

import com.collectivecycling.fit2thrift.Fit
import com.collectivecycling.fit2thrift.thrift.fit.FitData
import org.apache.hadoop.mapreduce.Job
import org.apache.logging.log4j.LogManager
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.thrift.ParquetThriftOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by nick on 10/10/16.
  */
object Fit2Parquet {
  val logger = LogManager.getLogger(Fit2Parquet.getClass.getName.split("\\$").last)

  val sparkConf = new SparkConf()
    .setAppName("Fit2Parquet")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]) {

    logger.debug("start")
    if (args.length != 2) {
      System.err.println("Usage: Fit2Parquet <<directory containing *.fit files>> <<parquet file name>>")
      logger.debug("exit")
      sc.stop()
      System.exit(1)
    }

    val Array(fitDirName, parquetFileName) = args

    logger.info (s"Using $fitDirName directory to create $parquetFileName file")

    val fitFiles = getFitFiles(fitDirName)

    val fitFileCount = fitFiles.size

    logger.debug(s"$fitFileCount .fit files")

    val fitDataCombined: List[FitData] = fitFiles.map(toFitThrift)

    writeParquet(parquetFileName,fitDataCombined)

    logger.info(s"stored $parquetFileName")

    sc.stop()

    logger.debug("finished")
  }

  def writeParquet(parquetFileName: String, fitDatum: Seq[FitData]): Unit = {
    val job = Job.getInstance()

    ParquetThriftOutputFormat.setThriftClass(job, classOf[FitData])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[FitData])

    sc.parallelize(fitDatum)
      .map(obj => (null, obj))
      .saveAsNewAPIHadoopFile(
        parquetFileName,
        classOf[Void],
        classOf[FitData],
        classOf[ParquetThriftOutputFormat[FitData]],
        job.getConfiguration())
  }

  def toFitThrift(file: File): FitData = {
    val name = file.toString
    logger.debug(s"Fit.toThrift $name")
    Fit.toThrift(name)
  }

  def getFitFiles(fitDirName: String): List[File] = {
    getListOfFiles(fitDirName).filter(_.getName.endsWith(".fit"))
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
