package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if (queryRectangle == null || pointString == null || queryRectangle.length < 0 || pointString.length < 0) {
      return false
    }
    val rectangleCoordinates = queryRectangle.split(",")
    if (rectangleCoordinates.length < 4) {
      return false
    }
    val rectCoordinates = new Array[Double](4)
    for ((str, index) <- rectangleCoordinates.zipWithIndex) {
      rectCoordinates(index) = str.toDouble
    }
    val rectX1:Double = Math.min(rectCoordinates(0), rectCoordinates(2))
    val rectY1:Double = Math.min(rectCoordinates(1), rectCoordinates(3))
    val rectX2:Double = Math.max(rectCoordinates(0), rectCoordinates(2))
    val rectY2:Double = Math.max(rectCoordinates(1), rectCoordinates(3))
    val pointCoordinates = pointString.split(",")
    if (pointCoordinates.length < 2) {
      return false
    }
    val pointX:Double = pointCoordinates(0).toDouble
    val pointY:Double = pointCoordinates(1).toDouble
    if (pointX < rectX1 || pointX > rectX2 || pointY < rectY1 || pointY > rectY2) {
      return false
    }

    // YOU NEED TO CHANGE THIS PART
    return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
