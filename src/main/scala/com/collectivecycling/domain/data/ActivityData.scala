package com.collectivecycling.domain.data

import com.collectivecycling.domain.types._
import org.joda.time.DateTime

/**
  * @author nick
  */
case class ActivityData
(
  activityId: ActivityId,
  source: String,
  activityTimestamp: Timestamp,
  activityDateTime: String,
  dayOfWeek: Int,
  monthOfYear: Int,
  year: Int,
  distance: Distance,
  elapsedTime: ElapsedTime,
  movingTime: MovingTime,
  caloriesBurnt: Calories,
  //startPosition: DegreePosition,
  startLat: Double,
  startLong: Double,
  timezone: String
)