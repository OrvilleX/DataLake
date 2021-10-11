import org.apache.spark.sql.SparkSession
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

        /**
          * 基于原生API进行删除、更新与何必
          */
    }
}