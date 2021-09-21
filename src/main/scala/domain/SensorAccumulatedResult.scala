package domain

case class SensorAccumulatedResult(
                                    min: Int,
                                    averageAccumulator: AverageAccumulator,
                                    max: Int,
                                    successCounter: Int,
                                    errorCounter: Int)
