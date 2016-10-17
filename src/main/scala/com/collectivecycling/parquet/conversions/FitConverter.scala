package com.collectivecycling.parquet.conversions

import com.collectivecycling.domain.data.{ActivityData, MeasurementData, Position}
import com.collectivecycling.domain.types._
import com.collectivecycling.fit2thrift.thrift.fit.{FitData, Record, Session}
import org.joda.time.{DateTime, DateTimeZone}
import scala.collection.JavaConversions._

/**
  * @author nick
  */
object FitConverter {

  val multiplier: Double = Math.pow(2.0, 31.0) / 180.0d

  implicit def fitTimeToDateTime(fitTime: FitTime): DateTime = {
    new DateTime((fitTime * 1000) + com.garmin.fit.DateTime.OFFSET)
  }

  implicit def fitTimeToId(fitTime: FitTime): Id = {
    val fitDateTime: DateTime = fitTime
    fitDateTime.getMillis / 1000 // ID is seconds
  }

  implicit def fitTimeToTimestamp(fitTime: FitTime): Timestamp = {
    val fitDateTime: DateTime = fitTime
    fitDateTime.getMillis
  }

  implicit def semicirclesToDegrees(semicirclePosition: SemicirclePosition): DegreePosition = {
    new DegreePosition(semicirclePosition.latitude / multiplier, semicirclePosition.longitude / multiplier)
  }

  def getActivityId(session: Session): ActivityId = {
    fitTimeToId(session.startTime)
  }

  def getMeasurementId(record: Record): MeasurementId = {
    fitTimeToId(record.timestamp)
  }

  def getSource(fitData: FitData): String = {
    fitData.fileId.manufacturer.toString + ":" + fitData.fileId.product.toString + ":" + fitData.fileCreator.softwareVerion.toString
  }

  implicit def fit2ActivityData(fitData: FitData): ActivityData = {
    val timezone: String = "Australia/Brisbane" // TODO pass in timezone after looking it up


    val session = fitData.sessions.get(0)
    val startTime: FitTime = session.startTime

    val activityId: ActivityId = getActivityId(session)

    val source: String = getSource(fitData)
    //val activityTimestamp:Timestamp = new java.sql.Timestamp(activity.activityTimestamp)
    val activityTimestamp: Timestamp = fitTimeToTimestamp(startTime)
    //val activityDT = new DateTime(activityTimestamp.getTime,DateTimeZone.forID(timezone))
    val activityDT = new DateTime(activityTimestamp, DateTimeZone.forID(timezone))
    val activityDateTime: String = activityDT.toString() // SparkSQL cant cope with Joda Date Time
    val dayOfWeek = activityDT.dayOfWeek().get - 1 // Monday = 0
    val monthOfYear = activityDT.monthOfYear().get - 1 // Jan = 0
    val year = activityDT.year().get
    val distance: Distance = session.getTotalDistance
    val elapsedTime: ElapsedTime = session.getTotalElapsedTime.toLong
    val movingTime: MovingTime = session.getTotalTimerTime.toLong
    val caloriesBurnt: Calories = session.getTotalCalories
    val startPosition: DegreePosition = semicirclesToDegrees(new SemicirclePosition(session.getStartPositionLat, session.getStartPositionLong))

    ActivityData(activityId, source, activityTimestamp, activityDateTime, dayOfWeek, monthOfYear, year, distance, elapsedTime, movingTime, caloriesBurnt, startPosition.latitude, startPosition.longitude, timezone)
  }

  implicit def fit2MeasurementDataList(fitData: FitData): List[MeasurementData] = {
    val timezone: String = "Australia/Brisbane" // TODO pass in timezone after looking it up


    val session = fitData.sessions.get(0)

    implicit val source = getSource(fitData)
    implicit val activityId: ActivityId = getActivityId(session)


    val records: List[Record] =fitData.getRecords.toList
    val measurements: List[MeasurementData] = records.map(record2MeasurementData)

    measurements
  }

  implicit def record2MeasurementData(record: Record)(implicit source: String, activityId: ActivityId): MeasurementData = {
    // TODO the timezone can change over a ride

    //val source: String = ""
    //val activityId=0

    val measurementId: MeasurementId = getMeasurementId(record)
    val timestamp: Timestamp = fitTimeToTimestamp(record.timestamp)
    val distance: Distance = record.distance
    val altitude: Altitude = record.altitude
    val speed: Speed = record.speed
    val cadence: Cadence = record.cadence
    val heartRate: HeartRate = record.heartRate
    val airTemp: AirTemperature = record.temperature
    val position: Position = semicirclesToDegrees(new SemicirclePosition(record.positionLat, record.positionLong))

    MeasurementData (
      measurementId,
      source,
      activityId,
      timestamp,
      distance,
      altitude,
      speed,
      cadence,
      heartRate,
      airTemp,
      position.latitude,
      position.longitude
    )
  }

}