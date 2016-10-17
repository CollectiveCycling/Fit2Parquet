package com.collectivecycling.domain

import com.collectivecycling.domain.data.Position

/**
  * Created by nick on 14/10/16.
  */
object types {
  type FitTime = Long
  type Timestamp = Long
  type ActivityId = Id
  type MeasurementId = Id
  type Calories = Long
  type Speed = Double
  type Distance = Metres
  type Cadence = Short
  type Power = Int
  type HeartRate = Short
  type Altitude = Metres
  type AirTemperature = Byte
  type ElevationGain = Metres
  type MovingTime = Long
  type ElapsedTime = Long
  type SemicirclePosition = Position
  type DegreePosition = Position

  type Metres = Double
  type Id = Long
}
