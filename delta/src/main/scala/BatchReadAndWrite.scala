import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
/**
  * 批量读写操作
  */
object BatchReadAndWrite {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("delta").setMaster("local")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        /**
          * 读取历史版本数据
          * 1. 基于时间戳
          * 2. 基于文件版本号
          */
        // val df1 = spark.read.format("delta").option("timestampAsOf", "12002112313")
        //     .load("./tmp/delta-table")
        // val df2 = spark.read.format("delta").option("versionAsOf", 1)
        //     .load("./tmp/delta-table")
        
        /**
          * 追加&覆盖数据
          * 通过mode切换overwrite覆盖或append追加
          */
        val df = spark.read.format("delta").load("./tmp/delta-table")
        val data = spark.range(50, 55)
        data.write.format("delta").mode("append").save("./tmp/delta-table")
        df.show()

        /**
          * 条件覆盖
          * 如果写入的元数据与实际存储的存在差异性，可以通过
          * option("mergeSchema", "true") 合并元数据
          * option("overwriteSchema", "true") 覆盖元数据
          */
        data.write.format("delta").mode("overwrite")
            .option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'").save("./tmp/delta-table")
        
        
    }
}