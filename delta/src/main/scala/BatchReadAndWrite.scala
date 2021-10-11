import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
/**
  * 批量读写操作
  */
object BatchReadAndWrite {
    def main(args: Array[String]) {
        val spark = SparkSession.builder()
            .appName("delta")
            .master("local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()

        /**
          * DeltaTableBuilder自定义方式
          */
        // DeltaTable.createOrReplace(spark)
        //   .tableName("event")
        //   .addColumn("date", DateType)
        //   .addColumn("eventId", "STRING")
        //   .addColumn("eventType", StringType)
        //   .addColumn(
        //     DeltaTable.columnBuilder("data")
        //       .dataType("STRING")
        //       .comment("event data")
        //       .build()
        //   ).location("./tmp/event")
        //   .property("description", "table with event data")
        //   .execute()

        /**
          * 基于DataFrame读取数据
          */
        // val df = spark.read.format("delta").load("./tmp/delta-table")
        // df.select("id").where("id > 10").show()

        /**
          * 基于SQL读取数据
          */
        // spark.sql("CREATE TABLE testtab(id Long, name String) USING DELTA")
        // spark.sql("INSERT INTO testtab VALUES(1, '1'), (2, '2')")
        // spark.sql("SELECT * FROM testtab").show()

        // spark.sql("CREATE TABLE testtab USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab'")
        // 修改列备注与排序
        // spark.sql("ALTER TABLE testtab CHANGE COLUMN name2 name2 STRING COMMENT 'name2 com' FIRST")
        // spark.sql("DESC testtab").show()
        // spark.sql("SELECT * FROM testtab").show()
        // 增加新列
        // spark.sql("ALTER TABLE testtab ADD COLUMNS (name2 STRING)")

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
          * 追加
          */
        // val df = spark.read.format("delta").load("./tmp/delta-table")
        // val data = spark.range(50, 55)
        // data.write.format("delta").mode("append").save("./tmp/delta-table")
        // df.show()

        /**
          * 覆盖数据&条件覆盖
          * 如果写入的元数据与实际存储的存在差异性，可以通过
          * option("mergeSchema", "true") 合并元数据
          * option("overwriteSchema", "true") 覆盖元数据
          */
        // val df = spark.read.format("delta").load("./tmp/delta-table")
        // val data = spark.range(0, 10)
        // data.write.format("delta").mode("overwrite").save("./tmp/delta-table")
        // data.write.format("delta").mode("overwrite")
        //     .option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'").save("./tmp/delta-table")
                
    }
}