package com.test.structured_streaming.mission;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import java.io.Serializable;
import java.sql.Timestamp;

@Component
/**
 * 连续处理模式，
 * 实现ApplicationRunner接口，
 * 则可随项目启动执行
 */
public class ContinuousProcessingApplicaitonRunnerImpl implements   Serializable {



     public void run(ApplicationArguments args) throws Exception {

        //可以设置sparkcon，然后加载
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructured")
                .master("local[*]")//本地模式
                .getOrCreate();

      spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "cdh1:9092")
                .option("subscribe", "topic1")//订阅主题
                .option("startingOffsets", "earliest")//从最早开始消费
                .load();
         df.printSchema();

        Dataset<Row> rowDataset = df.selectExpr("CAST(value AS STRING)","timestamp");
        rowDataset.writeStream().foreach(

                new ForeachWriter<Row>() {


                    @Override
                    public boolean open(long partitionId, long version) {

                        return true;//返回true，系统调用 process ，然后调用 close
                    }

                    @Override

                    public void process(Row value1) {
                        Timestamp startTime=value1.getTimestamp(1);
                        String record = value1.getString(0);
                        System.out.println("每一条记录："+record); //这里打印的就是kafka里面的每一条记录
                        long endTime = System.currentTimeMillis();
                        System.out.println("相差时间"+(endTime - startTime.getTime()) + "ms");
                        //经过测试，这里的时间差绝大部分都在500ms左右，可能这是我电脑的性能问题吧
                        //这个时间取的也有问题，记得有个参数，可以自动添加接收到数据的时间，但是忘记了
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        //如果失败可以回滚事务
                    }
                }
        ).outputMode("update")
                .trigger(Trigger.Continuous(10))//Trigger 10ms,这里是持续处理，超低延迟，获取不到数据便会输出warn等级的日志
                .start()
                .awaitTermination();
                 //开始连续处理查询之前，必须确保群集中有足够的核心并行执行所有任务。 例如，如果您正在读取具有10个分区的Kafka主题，则群集必须至少具有10个核心才能使查询正常执行。










//        df.printSchema();
//        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp")    //加上这个，还是二进制码, binary
//                .groupBy(functions.window(functions.col("timestamp"), "1 minutes", "5 minutes"),
//                functions.col("columnA"));
//            .groupBy("value").count();
//            .groupBy("value").agg(org.apache.spark.sql.functions.count(functions.lit(1)))
//                .writeStream().format("console").outputMode("complete").start().awaitTermination();
//        df.createOrReplaceTempView("t");
//
//
//
//        String sql = " SELECT * from t".toString().trim();
//        Dataset ds = spark.sql(sql);
//        StreamingQuery query = df.writeStream()
//                .outputMode("update")
//                .format("console")
//                .start();
//


    }


}