package com.collectivecycling.parquet

import com.collectivecycling.domain.data.{ActivityData, MeasurementData}
import com.collectivecycling.fit2thrift.thrift.fit.FitData
import com.collectivecycling.parquet.conversions.FitConverter
import org.apache.hadoop.mapreduce.Job
import org.apache.logging.log4j.LogManager
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.thrift.{ParquetThriftInputFormat, ThriftReadSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Created by nick on 14/10/16.
  */
object CleanseFitParquet {

  val logger = LogManager.getLogger(CleanseFitParquet.getClass.getName.split("\\$").last)

  val spark = SparkSession
    .builder()
    .appName("Cleanse Fit Parquet")
    .master("local[*]")
    //.config("spark.some.config.option", "some-value")
    .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]) {

    logger.debug("start")
    if (args.length != 3) {
      System.err.println("Usage: CleanseFitParquet <<fit parquet file name>> <<cleansed activities parquet file name>> <<cleansed measurements parquet file name>>")
      logger.debug("exit")
      spark.stop()
      System.exit(1)
    }

    val Array(fitParquetFileName, cleansedActivitiesFileName, cleansedMeasurementsFileName) = args

    val fitParquetData: RDD[FitData] = loadParquetData(spark, fitParquetFileName)

    val (activitiesDS,measurementsDS)  = runFitCleanse(spark, fitParquetData)

    //val activitiesDS: Dataset[ActivityData] = fd.map[ActivityData](FitConverter.fit2ActivityData).toDS

    logger.info(s"begin activities summary")
    val activitiesCount = activitiesDS.count()
    logger.info(s"have $activitiesCount activities")
    activitiesDS.show()
    activitiesDS.printSchema()
    logger.info(s"writing $cleansedActivitiesFileName activities parquet file")
    activitiesDS.write.mode(SaveMode.Overwrite).format("parquet").save(cleansedActivitiesFileName)

    //val measurementsDS: Dataset[MeasurementData] = fd.map(FitConverter.fit2MeasurementDataList).collect.flatten.toList.toDS

    logger.info(s"begin measurements summary")
    val measurementsCount = measurementsDS.count()
    logger.info(s"have $measurementsCount measurements")
    measurementsDS.show()
    measurementsDS.printSchema()
    logger.info(s"writing $cleansedMeasurementsFileName measurements parquet file")
    measurementsDS.write.mode(SaveMode.Overwrite).format("parquet").save(cleansedMeasurementsFileName)

    spark.stop()

    logger.debug("finished")
  }

  def loadParquetData(spark: SparkSession, fitParquetFileName: String): RDD[FitData] = {

    // NOTE Spark SQL 2.0 unable to decode nested parquet
    //val fitdf = spark.read.parquet ("hdfs://fig:8020/user/nick/fit_store/hortovanyi.parquet")

    val job = Job.getInstance()

    ParquetInputFormat.setReadSupportClass(
      job,
      classOf[ThriftReadSupport[FitData]])
    //job.getConfiguration.set("parquet.thrift.column.filter", "metadata")
    val parquetData: RDD[FitData] = spark.sparkContext.newAPIHadoopFile(fitParquetFileName,
      classOf[ParquetThriftInputFormat[FitData]],
      classOf[Void],
      classOf[FitData],
      job.getConfiguration).map { case (void, obj) => obj }

    parquetData.cache()

    val count = parquetData.count()
    logger.info(s"Have $count entries in $fitParquetFileName")

    parquetData

  }

  def runFitCleanse(spark: SparkSession, fitParquetData: RDD[FitData]) :(Dataset[ActivityData],Dataset[MeasurementData] ) ={


    val fd = fitParquetData.filter((fitData) => fitData.isSetActivities).cache()
    val activitiesDS = fd.map[ActivityData](FitConverter.fit2ActivityData).toDS().cache()
    val measurementsDS = fd.map(FitConverter.fit2MeasurementDataList).collect.flatten.toList.toDS().cache()
    (activitiesDS,measurementsDS)
  }

}
