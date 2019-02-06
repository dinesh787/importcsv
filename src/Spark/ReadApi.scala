

 package Spark
   import org.apache.spark.sql.{Row, SQLContext}
   import org.apache.spark.{SparkConf, SparkContext}
   import org.apache.spark.sql.functions._

   import org.apache.spark.sql.hive.HiveContext
   import org.apache.spark.sql.{SQLContext, SparkSession}
   import org.apache.spark.sql.SQLContext
   import org.apache.spark.sql.SparkSession
   import org.apache.spark.sql.functions.explode

   object ReadApi {

    def main(args: Array[String]) {

     val spark = SparkSession.builder()

       .appName("xxxxx")

       //.config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/mahendra/data")
       .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
       //stard hadoop (---start-all.sh----) and start thrift server(----hive --service metastore---) for enable hive in spark(
       .master("local[*]")
       .enableHiveSupport()
       .getOrCreate()
     // val spark = SparkSession.builder().appName("xxxxx").enableHiveSupport().getOrCreate()
     //val spark = SparkSession.builder().appName("xxxxx").enableHiveSupport().getOrCreate

     val sqlcontext: SQLContext = spark.sqlContext
     import sqlcontext.implicits._
     val result = scala.io.Source.fromURL("https://data.cityofnewyork.us/api/views/kku6-nxdu/rows.json").mkString
     // val events = sc.parallelize(result)

     val events =  spark.sparkContext.parallelize( result:: Nil)

     val df = sqlcontext.read.json(events)
     //df.printSchema

     val view = df.select("meta.view")

     view.printSchema()

     val data = view.withColumn("data", explode ( col("view.columns"))).select("data")
     data.printSchema()
      val data1 = data.select("data.cachedContents")
    data1.printSchema()

    val data2 = data1.select("cachedContents.top")
    // data3 = data2.withColumn("top" , explode( col("cachedContents.top")))
    val data3 =  data1.withColumn("top" , explode( col("cachedContents.top")))
    //data3.printSchema()
    val data4 = data3.select("top.count","top.item")

   // data4.printSchema()

      data4.createOrReplaceTempView("rest_api")
    spark.sql("SELECT * FROM rest_api").show(100)
   //spark.sql( "INSERT INTO TABLE API_partitionTable select * from rest_api" )

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")

    //spark.sql("INSERT INTO TABLE API_partitionTable(Curr_date) select * from rest_api  ")
   // spark.sql("INSERT INTO TABLE table_partition  partition(curr_date )  SELECT *,'2018-11-12' FROM  rest_api ")

  //  spark.sql("INSERT INTO TABLE new_partitiontable_api  partition(curr_date )  SELECT *,CASE  WHEN count <= 10 AND 0<>10 THEN '0-10' WHEN count <= 20 AND 0<> 20 THEN '11-20' ELSE 'The quantity is something else'  END  FROM table_partition WHERE curr_date = '2018-11-15'  AND  (count % 2 ) = 0 ");
   //spark.sql("INSERT INTO TABLE partitionTable_api  partition(curr_date )  SELECT count,item,CASE  WHEN count <= 10 AND 0<>10 THEN '0-10' WHEN count <= 20 AND 0<> 20 THEN '11-20' ELSE 'The quantity is something else'  END,curr_date  FROM table_partition WHERE curr_date = '2018-11-12'  AND  (count % 2 ) = 0 ");
   //spark.sql("INSERT INTO TABLE fact_date  partition(curr_date )  SELECT COUNT(*),rank,curr_date FROM partitionTable_api GROUP BY rank,curr_date ")
   spark.sql(" SELECT fact_date.rank,fact_date.curr_date ,COUNT(fact_date.count) from fact_date INNER JOIN partitionTable_api ON fact_date.rank = partitionTable_api.rank AND fact_date.curr_date = partitionTable_api.curr_date GROUP BY fact_date.rank,fact_date.curr_date");
  }
}