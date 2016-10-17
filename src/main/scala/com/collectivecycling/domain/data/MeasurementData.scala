package com.collectivecycling.domain.data

import com.collectivecycling.domain.types._

case class MeasurementData
(
  measurementId: MeasurementId,
  source: String,
  activityId: ActivityId,
  timestamp: Timestamp,
  distance: Distance, // meters
  altitude: Altitude, // meters
  speed: Speed,
  cadence: Cadence, // revolutions per minute
  heartRate: HeartRate, // Beats per minute
  airTemp: AirTemperature, // Air temperature in Celsius
  //position: Position
  posLat: Double,
  posLong: Double
)
    