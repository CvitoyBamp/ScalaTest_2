package ByMethods

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, count, max, mean, min, stddev}

object DataSet extends App {

  case class TaxiShema(
                        VendorID: Int,
                        tpep_pickup_datetime: String,
                        tpep_dropoff_datetime: String,
                        passenger_count: Int,
                        trip_distance: Double,
                        RatecodeID: Int,
                        store_and_fwd_flag: String,
                        PULocationID: Int,
                        DOLocationID: Int,
                        payment_type: Int,
                        fare_amount: Double,
                        extra: Double,
                        mta_tax: Double,
                        tip_amount: Double,
                        tolls_amount: Double,
                        improvement_surcharge: Double,
                        total_amount: Double
                      )

  case class TaxiZone(
                       LocationID: String,
                       Borough: String,
                       Zone: String,
                       service_zone: String
                     )

  def readParquet(path: String)(implicit spark: SparkSession) = spark.read.load(path)

  def readCSV(path: String)(implicit spark: SparkSession) =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def processTaxiData(taxiDF: DataFrame, taxiZoneDF: DataFrame) = {
    taxiDF
      .join(taxiZoneDF, taxiDF("DOLocationID") === taxiZoneDF("LocationID"), "left")
      .groupBy(taxiZoneDF("Borough"))
      .agg(
        count(taxiZoneDF("Borough")) as "Количество поездок",
        mean(taxiDF("trip_distance")) as "Среднее расстояние",
        stddev(taxiDF("trip_distance")) as "Среднеквадратичное отклонение",
        min(taxiDF("trip_distance")) as "Максимальная дистанция",
        max(taxiDF("trip_distance")) as "Минимальная дистанция"
      )
      .sort(col("Среднее расстояние").desc)
  }

}
