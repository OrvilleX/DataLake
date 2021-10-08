import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions


object HelloWorld {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("ScalaSparkML").setMaster("local")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // val data = spark.range(0, 5)
        // data.write.format("delta").save("./tmp/delta-table")

        /**
          * 数据全覆盖
          */
        // val df = spark.read.format("delta").load("./tmp/delta-table")
        // val data = spark.range(5, 10)
        // data.write.format("delta").mode("overwrite").save("./tmp/delta-table")
        // df.show()

        /**
          * 条件更新
          */
        // val deltaTable = DeltaTable.forPath("./tmp/delta-table")

        // deltaTable.update(
        //     condition = functions.expr("id % 2 == 0"),
        //     set = Map("id" -> functions.expr("id + 100"))
        // )
        // deltaTable.delete(condition = functions.expr("id % 2 == 0"))

        // val newData = spark.range(0, 20).toDF()

        // deltaTable.as("oldData")
        //     .merge(newData.as("newData"),
        //     "oldData.id = newData.id")
        // .whenMatched()
        // .update(Map("id" -> functions.col("newData.id")))
        // .whenNotMatched()
        // .insert(Map("id" -> functions.col("newData.id")))
        // .execute()

        // deltaTable.toDF.show()

        /**
          * 读取历史版本数据
          */
        val df = spark.read.format("delta").option("versionAsOf", 0).load("./tmp/delta-table")
        df.show()

        
    }
}