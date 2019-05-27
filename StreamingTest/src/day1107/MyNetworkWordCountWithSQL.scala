package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyNetworkWordCountWithSQL {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)     
    
     //创建一个StreamingContext对象，以local模式为例
    // 注意：保证cpu核数大于2,setMaster("local[2]")开启两个线程，相当于cpu核数设置为2
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]") 
    //两个参数：1，conf参数， 2：采样时间间隔:每隔3秒
    val ssc = new StreamingContext(conf,Seconds(3))
    
    //创建DStream，从netcat服务器接收数据
    val lines = ssc.socketTextStream("192.168.153.133", 1234, StorageLevel.MEMORY_ONLY)
    
     //进行单词计数
    //分词

    val words = lines.flatMap(_.split(" "))    
    
    //集成SparkSQL,使用SQL语句
    words.foreachRDD(rdd =>{
        //创建一个SparkSession对象
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        
        //把RDD转成一个dataframe
        import spark.implicits._
        val df1 = rdd.toDF("word")  //------> 表df1 ：只有一个word列
        
        //创建视图
        df1.createOrReplaceTempView("words")
        
        //执行SQL，通过SQL执行WordCount
        spark.sql("select word,count(*) from words group by word").show
    })
    
    ssc.start()
    
    ssc.awaitTermination()
    
  }
}

