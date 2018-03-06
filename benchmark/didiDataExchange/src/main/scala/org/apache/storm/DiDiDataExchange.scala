package org.apache.storm

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate org.apache.storm
  * Created by mastertj on 2018/3/6.
  * 将滴滴数据进行转换
  */
object DiDiDataExchange {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("spark://ubuntu2:7077").setAppName("DiDiDataExchange")
        val sc = new SparkContext(conf)
        val gps=sc.textFile("/user/root/TJ/DiDiData/gps")
        val order=sc.textFile("/user/root/TJ/DiDiData/order")
        //orderID  driverId
        val gpsRDD=gps.map(x=>{
            val strings=x.split(",")
            (strings(1),strings(0))
        }).distinct()

        //orderId   time
        val orderRDD=order.map(x=>{
            val strings=x.split(",")
            (strings(0),strings(1))
        }).distinct()

        //orderId (time driverId)
        val joinRDD=orderRDD.leftOuterJoin(gpsRDD).filter(x=>{
            x._2._2.isDefined
        }).sortByKey().distinct(8)

        val driversId=joinRDD.map(_._2._2.get)
        driversId.saveAsTextFile("hdfs://ubuntu2:9000/user/root/TJ/DiDiData/output/driverId")

        joinRDD.map(x=>{
            ""+x._1+","+x._2._1+","+x._2._2.get
        }).saveAsTextFile("hdfs://ubuntu2:9000/user/root/TJ/DiDiData/output/orders")
    }
}
