# Delta Lake教程  

当前的环境数据如下所示：  

* Scala：2.12
* Spark：3.1.1  
* Delta：1.0.0  

## 一、 表批量读取与写入  

### 1. 创建表  

首先我们可以基于Spark SQL创建采用Delta格式创建的表，其中我们可以使用基于地址的方式或采用默认地址的方式进行保存。  

```sql
CREATE IF NOT EXISTS TABLE events (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING DELTA

CREATE OR REPLACE TABLE events (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING DELTA
```  

下述则采用手动指定地址的方式进行创建。  

```sql
CREATE OR REPLACE TABLE delta.`/delta/events` (
  date DATE,
  eventId STRING,
  eventType STRING,
  data STRING)
USING DELTA
```

其中我们可以看到SQL语句中主要在原有基础上增加了`USING DELTA`来指定使用该方式。当然还有最简单普遍的方式
就是在我们完成数据处理后由DataFrame中的Write实现写入，具体的使用场景如下。  

```scala
data.write.format("delta").save("./tmp/delta-table")
df.write.format("delta").saveAsTable("events")
```  

最后一种方式就是其提供的`DeltaTableBuilder`API进行表的创建操作，当然在正常情况下不应该使用该类操作方式
进行处理。  

```scala
DeltaTable.createOrReplace(spark)
    .tableName("event")
    .addColumn("date", DateType)
    .addColumn("eventId", "STRING")
    .addColumn("eventType", StringType)
    .addColumn(
    DeltaTable.columnBuilder("data")
        .dataType("STRING")
        .comment("event data")
        .build()
    ).location("./tmp/event")
    .property("description", "table with event data")
    .execute()
```  

如果读者具备一定的Spark基础可以得知，数据还可以根据某一列进行分区，这里可以采用`DataFrame`或原生的方式指定分区列。  

```scala
df.write.format("delta").partitionBy("date").saveAsTable("events")

DeltaTable.createOrReplace(spark)
  .tableName("event")
  /*
   * 省略
   */
  .partitionedBy("date")
  .execute()
```  

### 2. 读取表  

下述我们将介绍几种读取数据的方式，具体如下所示。  

```scala
val df = spark.read.format("delta").load("./tmp/delta-table")
df.select("id").where("id > 10").show()
```  

当然这就是Delta的魅力，完全与Spark上层的API兼容，当然也有其独有的部分，可以根据数据
的版本进行读取，其支持基于时间戳与版本号两种方式进行读取，具体我们通过读取时的`option`实现。  

```scala
val df1 = spark.read.format("delta").option("timestampAsOf", "12002112313")
    .load("./tmp/delta-table")
val df2 = spark.read.format("delta").option("versionAsOf", 1)
    .load("./tmp/delta-table")
```  

对于已经存在的数据，并且存在对应元数据，而我们希望通过SQL进行访问，那么我们可以采用下述方式进行访问。  

```scala
spark.sql("CREATE TABLE testtab USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab'")
spark.sql("SELECT * FROM testtab").show()
```

### 3. 写入表  

由于Delta可以满足数据更新操作以及追加所以我们在进行写入时候也主要分为两种模式进行，下述我们将列举具体的方式，首先是最简单的数据追加。  

```scala
val df = spark.read.format("delta").load("./tmp/delta-table")
val data = spark.range(50, 55)
data.write.format("delta").mode("append").save("./tmp/delta-table")
```  

完成数据的追加之后我们就要考虑如何进行数据的更新，与其说是更新不如说说是分为了全覆盖以及条件覆盖这两种形式，下述我们将全部进行列举进行介绍。  

```scala
val df = spark.read.format("delta").load("./tmp/delta-table")
val data = spark.range(0, 10)
data.write.format("delta").mode("overwrite").save("./tmp/delta-table")
data.write.format("delta").mode("overwrite")
    .option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'").save("./tmp/delta-table")
```  

对应也可以通过使用SQL进行数据的追加或覆盖写入。  

```sql
INSERT INTO testtab SELECT * FROM newEvents

INSERT OVERWRITE TABLE testtab SELECT * FROM newEvents
```

### 4. 元数据  

然后就是针对元数据的操作，主要就是修改列名称、类型或增加新列，其也主要分为SQL与DataFrame的形式进行变更，首先我们将介绍基于SQL的方式进行增加或更新。  

```sql
ALTER TABLE table_name ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)

ALTER TABLE table_name CHANGE [COLUMN] col_name col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name]
```  

下面我们就列举一个完整的例子进行介绍，包括数据表的创建插入以及表变更，并显示对应的元数据。  

```scala
spark.sql("CREATE TABLE testtab(id Long, name String) USING DELTA")
spark.sql("INSERT INTO testtab VALUES(1, '1'), (2, '2')")
// 修改列备注与排序
spark.sql("ALTER TABLE testtab CHANGE COLUMN name2 name2 STRING COMMENT 'name2 com' FIRST")

// 增加新列
spark.sql("ALTER TABLE testtab ADD COLUMNS (name2 STRING)")
```  

除了采用SQL方式进行元数据的修改，我们还可以基于DataFrame的方式进行元数据的更新，在最后Write进行写入的时候增加通过如下配置即可。  

* option("mergeSchema", "true")：合并元数据  
* option("overwriteSchema", "true")：覆盖元数据  

## 二、 表删除、更新与合并  

为了能够使Spark SQL支持Delta的语法，我们需要在创建Spark的时候调整对应的配置项，具体的初始化如下。  

```scala
val spark = SparkSession.builder()
    .appName("delta")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
```

### 1. 基于SQL方式  

SQL作为比较普遍且通用的方式，其实大多数语法与标准的SQL的使用方法是一样的，所以这里将直接列举具体的使用
方式的代码进行介绍。其中需要注意的就是`MERGE`的使用方式需要特别注意。  

```scala
spark.sql("CREATE TABLE testtab USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab'")
spark.sql("CREATE TABLE updates USING DELTA LOCATION 'G:/github/OrvilleX/datalake/delta/tmp/delta-table'")

// 删除数据
spark.sql("DELETE FROM testtab WHERE id < 20")
spark.sql("SELECT * FROM testtab").show()
// 更新数据
spark.sql("INSERT INTO testtab VALUES(1, '1', '2')")
spark.sql("UPDATE testtab SET name = 'update1' WHERE id = 1")
spark.sql("SELECT * FROM testtab WHERE id = 1").show()
// 合并插入
spark.sql("MERGE INTO testtab USING updates ON testtab.id = updates.id WHEN MATCHED THEN UPDATE SET testtab.name2 = testtab.name WHEN NOT MATCHED THEN INSERT (id, name, name2) VALUES(id, '10', '20')")
spark.sql("SELECT * FROM testtab").show()
```

关于`MERGE`的使用将在基于`DeltaTable`原生API的方式中进行介绍，从而便于读者的具体使用。  

### 2. 基于API方式  

如果读者希望使用强类型的方式进行编写，则可以使用其提供的原生API进行具体的操作访问，其提供了诸多的重构
方法来满足我们各类的使用需求，下述将只列举其中常用的部分进行说明。  

```scala
val table = DeltaTable.forPath(spark, "G:/github/OrvilleX/datalake/delta/spark-warehouse/testtab")
// 删除数据
table.delete("id < 20")
table.toDF.show()
// 更新数据
table.updateExpr(
  "id = 1",
  Map("name" -> "'update2'")
)
table.toDF.show()
// 合并插入
val target = DeltaTable.forPath(spark, "G:/github/OrvilleX/datalake/delta/tmp/delta-table")
table.as("testtab").merge(
  target.as("updates").toDF,
  "testtab.id = updates.id"
).whenMatched
.updateExpr(
  Map("name2" -> "testtab.name")
).whenNotMatched()
.insertExpr(
  Map(
    "id" -> "updates.id",
    "name" -> "'30'",
    "name2" -> "'30'"
  )
).execute()
table.toDF.show()
```  

`whenMatched`可以支持填写具体的条件，组成多个子句来进行匹配。如果不填写条件则当`merge`子句中的`testtab.id = updates.id`匹配  
时执行后续的操作，每个`whenMatched`最多存在一个`update`或者`delete`子句。如果读者只想进行全更新则可以使用`updateAll`实现。  
`whenNotMatched`一样可以支持填写具体的条件，组成多个子句来进行匹配。如果不填写条件则当merge不匹配的情况下执行后续的操作，最多  
只能存在一个`insert`子句用来进行数据插入，当然也可以直接使用`insertAll`进行全插入。这里需要注意的是`whenMatched`与`whenNotMatched`  
并不是一定要都出现，可以根据实际需要执行的情况进行调整。  
