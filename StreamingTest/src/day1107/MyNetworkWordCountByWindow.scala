package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level


object MyNetworkWordCountByWindow {
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
    val words = lines.flatMap(_.split(" ")).map((_,1))
    
    //每10秒，把过去的30秒的数据进行计数
    /* reduceFunc: 表示要进行的操作
     * windowDuration： 窗口的大小
     * slideDuration： 窗口滑动 距离
     * 
     * words.reduceByWindow(reduceFunc, windowDuration, slideDuration)
     * 
     * 
     */
    
    val result = words.reduceByKeyAndWindow((x:Int,y:Int)=>(x + y), Seconds(30), Seconds(9))
    
      //输出
    result.print()
    
    ssc.start()
    ssc.awaitTermination()
    
    
 }
}