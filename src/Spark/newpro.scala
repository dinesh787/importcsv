package Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql._


object newpro {


  def main(args: Array[String]) = {

    val spark = SparkSession.builder.appName("mapExample")
    .config("hive.metastore.uris", "thrift://localhost:9083")//stard hadoop (---start-all.sh----) and start thrift server(----hive --service metastore---) for enable hive in spark(
   //.config("hive.metastore.uris", "jdbc:mysql://localhost/metastore")
      //.config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/mahendra/data")
      //.config("hive.metastore.uris", "thrift://localhost:9000")

     .master("local[*]")
      //.config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sqlcontext: SQLContext = spark.sqlContext
    //val hiveCont = new org.apache.spark.sql.hive.HiveContext(sqlcontext)
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
    import sqlcontext.implicits._

    import spark.implicits._
   // val data = spark.sparkContext.textFile("/home/mahendra/Documents/new.csv")

   //val q =data.flatMap(x => x.split(" ")).foreach(println)
    //val n = data.first()
    //println(n)
    //val hiveContext: Nothing = new HiveContext(spark)

    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/mahendra/Documents/new.csv")
    //data.show()
    data.printSchema()
   data.createOrReplaceTempView("diff")
    spark.sql("SELECT * FROM diff").show(100)
   spark.sqlContext.sql("Insert into csvtable  SELECT * FROM diff")
    spark.sqlContext.sql(" select * from  csvtable  ").take(100).foreach(println)

   //println(q)
   // data.foreach(println)


 // val t = spark.read.csv("/home/mahendra/Documents/new.csv")
 //val sqlcontext: SQLContext = spark.sqlContext


  //val sqlContext = new org.apache.spark.sql.SQLContext(sqlcontext)





  }
}
