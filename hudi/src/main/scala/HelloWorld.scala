import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object HelloWorld {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("hudi").setMaster("local")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    }
}