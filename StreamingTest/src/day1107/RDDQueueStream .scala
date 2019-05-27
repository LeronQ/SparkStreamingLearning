package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD

object RDDQueueStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)    
    
    val conf = new SparkConf().setAppName("RDDQueueStream").setMaster("local[2]") 
    //创建一个StreamingContext对象，以local模式为例
    // 注意：保证cpu核数大于2,setMaster("local[2]")开启两个线程，相当于cpu核数设置为2
    val ssc = new StreamingContext(conf,Seconds(1))     
    
    //
    val rddQueue = new Queue[RDD[Int]]()
    for(i<- 1 to 3){
      rddQueue += ssc.sparkContext.makeRDD(1 to 10)
      //休眠1秒
      Thread.sleep(1000)
    }
    
    //从队列中接收数据，创建DStream
    val inputDStream = ssc.queueStream(rddQueue)
    
    //处理数据
    val result = inputDStream.map(x => (x,x*2))
    result.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}