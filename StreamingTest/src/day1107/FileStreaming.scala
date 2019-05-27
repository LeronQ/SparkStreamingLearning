package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level

object FileStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)    
    
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]") 
    // 两个参数：1，conf参数， 2：采样时间间隔:每隔3秒
    val ssc = new StreamingContext(conf,Seconds(3)) 
    
    //直接监控本地的某个目录，如果有新的文件产生，就读取进来，---》直接打印
    val lines = ssc.textFileStream("D:\\temp\\spark")
    //输出
    lines.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}