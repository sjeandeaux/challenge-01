package com.challenge.matching

case class MatchingDataLine(matchingId: String,
                            requestId: String,
                            requestLatitude: Double,
                            requestLongitude: Double,
                            driverId: String,
                            driverLatitude: Double,
                            driverLongitude: Double,
                            isMatch: Boolean,
                            computeAt: Long)

object MatchingDataLine {


  def apply(line: String): MatchingDataLine = {

    val Array(matchingId, requestId, requestLatitude, requestLongitude, driverId, driverLatitude, driverLongitude, isMatch, computeAt) = line.split(",")

    MatchingDataLine(matchingId,
      requestId,
      requestLatitude.toDouble,
      requestLongitude.toDouble,
      driverId,
      driverLatitude.toDouble,
      driverLongitude.toDouble,
      isMatch.toBoolean,
      computeAt.toLong
    )
  }
}


