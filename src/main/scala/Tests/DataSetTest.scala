package Tests

import ByMethods.DataSet._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession


class DistanceDistributionTest extends SharedSparkSession{

  implicit val spark = SparkSession.builder()
    .appName("Задание №3  Dataset")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val csvPath: String = "D:\\scala\\DataFrame\\src\\main\\resources\\data\\taxi_zones.csv"

  val parquetPath: String = "D:\\scala\\DataFrame\\src\\main\\resources\\data\\yellow_taxi_jan_25_2018"

  val taxiDF = readCSV(csvPath).as[TaxiZone].toDF

  val taxiZoneDF = readParquet(parquetPath).as[TaxiShema].toDF


  val finalTable = processTaxiData(taxiDF, taxiZoneDF)

  checkAnswer(
    finalTable,
    Row("Staten Island", 62, 20.114032258064512, 6.892080858225576, 0.3, 33.78) ::
      Row("EWR", 491, 17.559816700610995, 3.761422354588497, 0.01, 45.98) ::
      Row("Bronx", 1562, 9.209398207426394, 5.330517246526124, 0.02, 31.18) ::
      Row("Unknown", 6285, 3.68733333333335, 5.715976934424563, 0.01, 66.0) ::
      Row("Brooklyn", 12587, 6.932374672280937, 4.754780022484276, 0.01, 44.8) ::
      Row("Queens", 13394, 8.98944004778256, 5.420778528649564, 0.01, 51.6) ::
      Row("Manhattan", 295642, 2.1884251899256144, 2.6319377112494804, 0.01, 37.92)  :: Nil
  )

}