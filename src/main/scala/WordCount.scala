import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
// Spark


object WordCount {

  private val AppName = "WordCountJob"

  // Run the word count. Agnostic to Spark's current mode of operation: can be run from tests as well as from main
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("Test")
      .getOrCreate()

    // read input file
    val linesDF = spark.read.text(args(0))

    // count words
    val wordCountDF = wordCount(linesDF, spark)

    // show the count
    wordCountDF.orderBy(desc("count")).show(truncate=false)

  }

  def wordCount(lines: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val wordsDF = lines.select(split(lines("value"), " ").alias("words"))

    val wordDF = wordsDF.select(explode(wordsDF("words"))
      .alias("word"))

    wordDF.groupBy("word").count
  }
}