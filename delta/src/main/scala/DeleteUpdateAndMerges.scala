import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
object  DeleteUpdateAndMerges {
    def main(args: Array[String]) {
        val spark = SparkSession.builder()
            .appName("delta")
            .master("local")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()

        /**
          * 基于SQL方式进行删除、更新与合并
          */
        spark.sql("CREATE TABLE testtab USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab'")
        // spark.sql("CREATE TABLE updates USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/tmp/delta-table'")
        // 删除数据
        // spark.sql("DELETE FROM testtab WHERE id < 20")
        // spark.sql("SELECT * FROM testtab").show()
        // 更新数据
        // spark.sql("INSERT INTO testtab VALUES(1, '1', '2')")
        // spark.sql("UPDATE testtab SET name = 'update1' WHERE id = 1")
        // spark.sql("SELECT * FROM testtab WHERE id = 1").show()
        // 合并插入
        // spark.sql("MERGE INTO testtab USING updates ON testtab.id = updates.id WHEN MATCHED THEN UPDATE SET testtab.name2 = testtab.name WHEN NOT MATCHED THEN INSERT (id, name, name2) VALUES(id, '10', '20')")
        // spark.sql("SELECT * FROM testtab").show()
        // 查询所有变更记录与最新记录
        // spark.sql("DESCRIBE HISTORY testtab").show()
        // spark.sql("DESCRIBE HISTORY testtab LIMIT 1").show()

        /**
          * 基于原生API进行删除、更新与合并
          */
        val table = DeltaTable.forPath(spark, "G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab")
        // 删除数据
        // table.delete("id < 20")
        // table.toDF.show()
        // 更新数据
        // table.updateExpr(
        //   "id = 1",
        //   Map("name" -> "'update2'")
        // )
        // table.toDF.show()
        // 合并插入
        // val target = DeltaTable.forPath(spark, "G:/github/OrvilleX/datalake/delta/tmp/delta-table")
        // table.as("testtab").merge(
        //   target.as("updates").toDF,
        //   "testtab.id = updates.id"
        // ).whenMatched
        // .updateExpr(
        //   Map("name2" -> "testtab.name")
        // ).whenNotMatched()
        // .insertExpr(
        //   Map(
        //     "id" -> "updates.id",
        //     "name" -> "'30'",
        //     "name2" -> "'30'"
        //   )
        // ).execute()
        // table.toDF.show()
        /**
          * `whenMatched`可以支持填写具体的条件，组成多个子句来进行匹配。如果不填写条件则当merge子句中的`testtab.id = updates.id`匹配
          * 时执行后续的操作，每个`whenMatched`最多存在一个`update`或者`delete`子句。如果读者只想进行全更新则可以使用`updateAll`实现。
          * `whenNotMatched`一样可以支持填写具体的条件，组成多个子句来进行匹配。如果不填写条件则当merge不匹配的情况下执行后续的操作，最多
          * 只能存在一个`insert`子句用来进行数据插入，当然也可以直接使用`insertAll`进行全插入。这里需要注意的是`whenMatched`与`whenNotMatched`
          * 并不是一定要都出现，可以根据实际需要执行的情况进行调整。
          */
        // 查询所有变更记录与最新记录
        // table.history().show()
        // table.history(1).show()
    }
}