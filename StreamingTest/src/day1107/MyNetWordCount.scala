package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
/*
 * * 知识点（）：
 * 1：创建一个StreamingContext ，核心创建一个DStream（离散流）
 * 2：DStream表现形式：就是一个RDD
 * 3：使用DStream把连续的数据流变成不连续的RDD
 */
object MyNetWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF) 
     //创建一个StreamingContext对象，以local模式为例
    // 注意：保证cpu核数大于2,setMaster("local[2]")开启两个线程，相当于cpu核数设置为2
       val conf = new SparkConf().setAppName("MyNetWordCount").setMaster("local[2]")
       
        // 两个参数：1，conf参数， 2：采样时间间隔:每隔3秒
       val ssc = new StreamingContext(conf,Seconds(3))
       
       //创建DStream，从netcat服务器接收数据
       val lines = ssc.socketTextStream("192.168.152.133",1234,StorageLevel.MEMORY_ONLY)
       
      //进行单词计数
      // 分词
      val words = lines.flatMap(_.split(" "))
      
      //计数
//      val wordCount=words.map((_,1)).reduceByKey(_+_)
      
      // 使用transform来完成mao一样的结果
      val wordPair = words.transform(x =>x.map(x=>(x,1)))
      
      
      //打印结果
      wordPair.print()
      
      // 启动StreamingContext进行计算
      ssc.start();
    
      //等待任务结束
      ssc.awaitTermination()
         
  }
}