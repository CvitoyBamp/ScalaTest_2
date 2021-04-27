import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import RDDs.{TaxiShema}
import java.time.{LocalTime, LocalDate}

class SimpleUnitTest extends AnyFlatSpec {
  implicit val spark = SparkSession.builder()
    .appName("Задание №3 RDDtest")
    .master("local[*]")
    .getOrCreate()

  it should "upload data from path and proccees" in {
    val taxiDF: DataFrame = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

    import spark.implicits._

    val taxiDS: Dataset[TaxiShema] = taxiDF.as[TaxiShema]

    val taxiRDD: RDD[TaxiShema] = taxiDS.rdd

    val taxiFinal: RDD[(String, Int)] = taxiRDD.map(x => (x.tpep_pickup_datetime, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)

    val result = taxiFinal.take(1)

    assert(result(0)._1.drop(11) == LocalTime.of(9,51,56).toString)
    assert(result(0)._2 == 18)
  }
}
