package Tests

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import java.time.{LocalDate, LocalTime}

import ByMethods.RDDs.{ResultTable, TaxiShema, readParquet}

class SimpleUnitTest extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .appName("Задание №3 RDDtest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val parquetPath: String = "D:\\scala\\DataFrame\\src\\main\\resources\\data\\yellow_taxi_jan_25_2018"

  it should "upload data from path and proccees" in {

    val taxiRDD = readParquet(parquetPath).as[TaxiShema].rdd

    val result = ResultTable(taxiRDD).take(1)

    assert(result(0)._1.drop(11) == LocalTime.of(9,51,56).toString)
    assert(result(0)._2 == 18)
  }
}

