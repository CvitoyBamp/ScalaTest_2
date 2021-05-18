package ByMethods

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import java.sql.Timestamp

object RDDs extends App{

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

  def readParquet(path: String)(implicit spark: SparkSession) = spark.read.load(path)


  def ResultTable(taxiRDD: RDD[TaxiShema]) = {
    taxiRDD.map(x => (x.tpep_pickup_datetime, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
  }

}