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

