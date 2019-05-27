package day1107

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MyTotalNetworkWordCount {
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
     //创建一个StreamingContext对象，以local模式为例
    // 注意：保证cpu核数大于2,setMaster("local[2]")开启两个线程，相当于cpu核数设置为2
    val conf = new SparkConf().setAppName("MyNetworkWordCount").setMaster("local[2]") 
    //两个参数：1，conf参数， 2：采样时间间隔:每隔3秒
    val ssc = new StreamingContext(conf,Seconds(3))
    
    //设置检查点目录，保存之前的状态
    ssc.checkpoint("hdfs://192.168.153.133:9000/sparkckpt")
    
    //创建DStream，从netcat服务器接收数据
    val lines = ssc.socketTextStream("192.168.153.133", 1234, StorageLevel.MEMORY_ONLY)
    
    //进行单词计数
    //分词
    val words = lines.flatMap(_.split(" "))
    // 每个单词记一次
    val wordPair = words.map(w => (w,1))
       
       // 定义一个值函数
       /*
        * 第一个参数：当前的值是多少
        * 第二个参数：之前的参数是多少
        */
    val addFunc = (curreValues:Seq[Int],previousValues:Option[Int])=>{
      // 进行累加运算
      //1:把当前值序列进行累加
      val currentTotal = curreValues.sum
      
      //2:在之前的值上再来累加，注意：如果之前没有值，就是0
       //返回
      Some(currentTotal + previousValues.getOrElse(0))
    }
    
    
    //进行累加计算
    val total = wordPair.updateStateByKey(addFunc)
    
    //输出
    total.print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}


//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.storage.StorageLevel
//
///*
// * * 知识点（）：
// * 1：创建一个StreamingContext ，核心创建一个DStream（离散流）
// * 2：DStream表现形式：就是一个RDD
// * 3：使用DStream把连续的数据流变成不连续的RDD
// */
//object MyTotalNetWordCount {
//  def main(args: Array[String]): Unit = {
//     //创建一个StreamingContext对象，以local模式为例
//    // 注意：保证cpu核数大于2,setMaster("local[2]")开启两个线程，相当于cpu核数设置为2
//       val conf = new SparkConf().setAppName("MyNetWordCount").setMaster("local[2]")
//       
//        // 两个参数：1，conf参数， 2：采样时间间隔:每隔3秒
//       val ssc = new StreamingContext(conf,Seconds(3))
//       
//       // 设置检查点目录，保存之前的状态
//       ssc.checkpoint("hdfs://192.168.153.133:9000/sparkckpt")
//       
//       //创建DStream，从netcat服务器接收数据
//       val lines = ssc.socketTextStream("192.168.152.133",1234,StorageLevel.MEMORY_ONLY)
//       
//       //进行单词计数
//       // 分词
//       val words = lines.flatMap(_.split(" "))
//       // 每个单词记一次
//       val wordPair =words.map(w =>(w,1))
//       
//       
//       // 定义一个值函数
//       /*
//        * 第一个参数：当前的值是多少
//        * 第二个参数：之前的参数是多少
//        */
//       
//       val addFunc =(currentValues:Seq[Int],previousValues:option[Int])=>{
//         // 进行累加运算
//           //1:把当前值序列进行累加
//         val currentTotal = currentValues.sum
//         
//           //2:在之前的值上再来累加，注意：如果之前没有值，就是0
//           //返回
//         Some(currentTotal + previousValues.getOrElse(0))
//         
//       }
//       
//       
//       //进行累加计算
//       val total= wordPair.updateStateByKey(addFunc)
//       
//       // 输出
//       total.print()
//       
//       ssc.start()
//       
//       ssc.awaitTermination()
//       
//  }
//}