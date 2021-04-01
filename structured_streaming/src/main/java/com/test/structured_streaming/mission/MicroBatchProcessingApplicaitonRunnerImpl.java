package com.test.structured_streaming.mission;


import com.alibaba.fastjson.JSONObject;
import com.test.structured_streaming.pojo.Record;


import com.test.structured_streaming.utils.HBaseUtils;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/3/15 16:22
 */
@Component
public class MicroBatchProcessingApplicaitonRunnerImpl  implements   Serializable {

    public Map<String,Object> map=new HashMap<String,Object>();
    @PostConstruct
    public void run( ) throws Exception {
        System.out.println("微批处理");
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructured")
                .master("local[*]") //本地模式,多线程并行处理
                .getOrCreate();


//        HBaseUtils.delByRowkey("ns1:t2","cf1","0");
//        HBaseUtils.delByRowkey("ns1:t2","cf1","1");
        //删除掉offset，测试结果是否正常

        //hbase获取offset
        Map<String, Object> oneRow0 = HBaseUtils.getOneRow("ns1:t2", "0");
        Map<String, Object> oneRow1 = HBaseUtils.getOneRow("ns1:t2", "1");
        String offset="{\"topic1\":{";
        if (oneRow0!=null){
              offset+="\"0\":"+oneRow0.get("offset");
            if(oneRow1!=null){
                offset+=",\"1\":"+oneRow1.get("offset")+"}}";

            }else{
                offset+="}}";
            }
        }  else{
            if(oneRow1!=null){
                offset+="\"1\":"+oneRow1.get("offset")+"}}";

            }else{
                offset="earliest";
            }

        }



//        String offset="{\"topic1\":{\"0\":"+oneRow0.get("offset")+",\"1\":"+oneRow1.get("offset")+"}}";
        System.out.println(offset);

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "192.168.43.101:9092")
                .option("subscribe", "topic1")//订阅主题
                .option("startingOffsets",offset)//earliest是从最早开始消费，write指定了checkpointlocation参数，后续消费从上次offset开始
                //如果同时指定startingOffsets和checkpointLocation，以checkpointLocation为准
                //根据业务逻辑，更适合使用startingOffsets，checkpointLocation放弃不使用
                .option("maxOffsetsPerTrigger", "100")//设置最大偏移量,如果是100，基本是每个分区取50条
                .load();
        df.printSchema();


//--------------------------------------------------------------------------
//流式转批处理，问题就是每次获取的都是数组类型，对于此种模式感觉莫名其妙，也可能是我不够了解，本打算不使用
//     使用这个foreachBatch，避开了不同分区需要和一的问题，但是需要转换
        Dataset<Row> dataset = df.selectExpr("CAST(value AS STRING)","partition","offset");

        dataset.writeStream().foreachBatch(

                new VoidFunction2(){

            @Override
            public void call(Object o, Object o2) throws Exception {
                Dataset<Row> dd= (Dataset<Row>) o;
                System.out.println("dd"+dd);
//                dd.createOrReplaceGlobalTempView("table");
//                spark.sql("select time,number from global_temp.table.value");
                List<Record> beanlist=new ArrayList<Record>();

                List<Row> list=dd.collectAsList(); //list就是每一批次的数据，现在需要排序，筛选
                System.out.println("一批"+list.size());

                list.stream().forEach(new Consumer<Row>() {
                    @Override
                    public void accept(Row row) {
                        Record record = JSONObject.parseObject((String) row.get(0), Record.class);
                        //原来的项目里，row.get(0)需要判断是不是jsonobject
                        record.setPartition((Integer) row.get(1));
                        record.setOffset((Long) row.get(2));
                        beanlist.add(record);
                    }
                });



                Collections.sort(beanlist, new Comparator<Record>() {
                    @Override
                    //先按时间升序排序，时间相同再比较number大小
                    public int compare(Record o1, Record o2) {
                        if (o1.getTime().compareTo(o2.getTime())>0){
                              return 1;
                        }
                        if (o1.getTime().compareTo(o2.getTime())==0){

                            if(Integer.valueOf(o1.getNumber())>Integer.valueOf(o2.getNumber())){
                                return 1;
                            }
                            if(Integer.valueOf(o1.getNumber())==Integer.valueOf(o2.getNumber())){
                                return 0;
                            }
                            return  -1;
                        }
//                        System.out.println(o1);
                        return -1;

                    }
                });

                //list转dataset
                Dataset<Record> records = spark.sqlContext().createDataset(beanlist, Encoders.bean(Record.class));
                System.out.println("records:"+records);
                records.createOrReplaceTempView("tb");
                Dataset<Row> rowDataset = spark.sql("select number,time,partition,offset from tb");//把name过滤，测试sql能否使用

                //这里如果要转换成Dataset<Record>,就要和record的字段相同，因为我舍弃了name字段，转换原来的类型就会报错：cannot resolve '`name`' given input columns
                //尝试实现一个不满参的构造方法,经过测试，不行
                //尝试使用@Nullable注解，经过测试，不行
                //估计只能新建一个没有name字段的类，舍弃
                List<Row> rowList = rowDataset.collectAsList();
                for(Row a :rowList){
                    //这个循环中存起来，我们并没有什么逻辑，直接存入hbase
//                    System.out.println(a);
                    map.put("number",a.get(0));
                    map.put("time",a.get(1));
                    HBaseUtils.putMapDataByRowkey("ns1:t2", "cf1", map,a.get(0)+""+a.get(1));
                    //感觉加上存入就慢了很多
                    //存完数据，存offset,因为只有两个分区
                    map.clear();
                    map.put("partition",a.get(2));
                    map.put("offset",a.get(3));
                    Iterator iter = map.entrySet().iterator();
                    while (iter.hasNext()) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        Object key = entry.getKey();
                        Object val = entry.getValue();
                        System.out.println(key+"=="+val);
                    }


                    HBaseUtils.putMapDataByRowkey("ns1:t2", "cf1", map,a.get(2).toString());
                    map.clear();
                    //这里有点细微的小问题，要保证数据和offset同时成功或者同时失败,省略不写了
                }
            }
        }
        )
//                .option("checkpointLocation", "./ck")//offset的检查点，记录每次的offset 这是存在项目中，生产中应该单独放置，需不需要HDFS目录暂且不清楚
                //上面的offset检查点，应该是不能使用的
                .start().awaitTermination();







//        -----------------------------------------------------------------------
        //下面这个模式，本应该将两个分区的数据和一，但是我做不到，这个方法也得放弃，再使用上面的
        //kafka有两个分区，接收的数据也是两个分区，但是foreach里面每次循环都是单独一个分区的，不知道这一批数据怎么合并

//        Dataset<Row> dataset = df.selectExpr("CAST(value AS STRING)","timestamp","partition");
//
//        StreamingQuery query = dataset.writeStream()
//                .foreach(new ForeachWriter<Row>() {
//
//                    Integer a=0;
//                    @Override
//                    public boolean open(long partitionId, long version) {
//                        long endTime = System.currentTimeMillis();
//                        System.out.println(endTime);
//                        return true;
//                    }
//
//                    @Override
//                    public void process(Row value) {
//
//
//                        String record = value.getString(0);
//                        Timestamp startTime=value.getTimestamp(1);
//                        System.out.println(record+"=="+value.get(2));
//
////                        System.out.println("相差时间"+(endTime - startTime.getTime()) + "ms");
//                        //在这个模式下测试，时间差为650ms左右，确实比持续处理要慢
//                        a++;
//                        //可以得到每一批的每一条数据
//                    }
//
//                    @Override
//                    public void close(Throwable errorOrNull) {
//                        System.out.println(a+"条");
//
//                    }
//                })
//                .outputMode("update")
//                .trigger(Trigger.ProcessingTime(2000))
//                .option("checkpointLocation", "./ck")//offset的检查点，记录每次的offset 这是存在项目中，生产中应该单独放置，需不需要HDFS目录暂且不清楚
//                 .start();
//        //经过测试，这个获取机制应该是轮询获取每个区的数据，同一批数据，不会来自不同的kafka分区
//
//        try {
//            query.awaitTermination();
//        } catch (StreamingQueryException e) {
//            e.printStackTrace();
//        }
//        -----------------------------------------------------------------------
        //这下面是console测试模式
//        StreamingQuery query = df
//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","timestamp","partition","offset")
//                .writeStream()
//                .outputMode("append")
//                .format("console")
//                .option("numRows",500)//每个触发器要打印的行数（默认值：20）
//                .option("truncate",false)//如果时间太长，是否截断输出（默认值：true）
//                .start();
//
//
//        query.awaitTermination();



    }

}
