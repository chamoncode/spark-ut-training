import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import com.github.mrpowers.spark.fast.tests.DatasetComparer


class WordCounTest extends AnyFunSuite with BeforeAndAfterAll with DatasetComparer {

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .getOrCreate()
  import spark.implicits._

  override def afterAll(): Unit = {
    super.afterAll()
    spark.close()
  }

  test("same words but not in the same order") {
    val file1 = spark.read.text(getClass.getResource("testfile").toString)
    val file2 = spark.read.text(getClass.getResource("testfile2").toString)

    assert(
      WordCount.wordCount(file1, spark)
        .except(WordCount.wordCount(file2, spark))
        .count == 0
    )
  }

  test("compare with fast spark tests") {
    val file1 = spark.read.text(getClass.getResource("testfile").toString)
    val file3 = spark.read.text(getClass.getResource("testfile3").toString)
    assertSmallDatasetEquality(WordCount.wordCount(file1, spark), WordCount.wordCount(file3, spark))
  }
}
