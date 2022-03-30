package agg

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType



object Spark_Agg {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nagg.Spark_Agg <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark_Agg").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    
    val linesrdd = sc.textFile(args(0))

    val rdd = linesrdd.map(line => {
      val users = line.split(",")
      val followedUser = users(1)
      (followedUser, 1)
    })
    
    // Original 
   //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1))reduceByKey(_ + _)
    //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1))reduceByKey(_ + _)
    //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1)).groupByKey().mapValues(nof => nof.reduce((x,y)=>x+y))

    // splits each line by (",").It filters all the user_ids that are divisible by 100.It then maps those user_ids and then aggregates the count on each user_id
    
    // using groupby RDD-G
   //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1)).groupByKey().mapValues(nof => nof.reduce((x,y)=>x+y))


    // using reducebykey RDD-R
    //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1))reduceByKey((x,y)=>x+y)


    // using foldbykey RDD-F
   //val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1)).foldByKey(0)((acc,ele) => {acc + ele})



    //using aggregatebykey RDD-A
   // val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1)).aggregateByKey(0)(_+_,_+_)

    // using DSET groupby
    //val dfFromRDD1 = linesrdd.toDF("user","follower")
    //val sparkSession =  SparkSession.builder().getOrCreate()
    //val dfFromRDD1 = SparkSession.createDataFrame(rdd).toDF("user", "follower")
    //val followercount = dfFromRDD1.groupby("user").sum("follower")

    
    val dfFromRDD1 = rdd.toDF("user","follower")
    val dfFromRDD2= dfFromRDD1.withColumn("user",col("user").cast(IntegerType))
    val counts = dfFromRDD2.filter($"user"%100 === 0).groupBy("user").agg(sum($"follower"))
    counts.show()
    //dfFromRDD2.printSchema()



    //logger.info(followercount.toDebugString)
    print(counts.explain(extended = true))
    //counts.rdd.map(_.toString())saveAsTextFile(args(1))
    //counts.write.csv(args(1))
    //counts.rdd.saveAsTextFile(args(1))
  }
}
