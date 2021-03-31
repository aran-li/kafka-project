package com.test.tokafka.mission;

import com.alibaba.fastjson.JSONArray;
import com.test.tokafka.TokafkaApplication;
import com.test.tokafka.pojo.Record;
import io.leopard.javahost.JavaHost;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.spark.Partition;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
public class ApplicaitonRunnerImpl implements ApplicationRunner {

    public SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static int i=0;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        Properties props = new Properties();
        InputStream inputStream= TokafkaApplication.class.getResourceAsStream("/vdns.properties");

        try {
            props.load(inputStream);
            System.out.println(props.get("cdh1"));
        } catch ( Exception e) {
            e.printStackTrace();
        }

        JavaHost.updateVirtualDns(props);
        JavaHost.printAllVirtualDns();// 打印所有虚拟DNS记录
        System.out.println("通过实现ApplicationRunner接口，在spring boot项目启动后打印参数");









//        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
//
//
//
//
//
//        properties.put("bootstrap.servers", "cdh2:9092,cdh3:9092");
//        properties.put("serializer.class",org.apache.kafka.common.serialization.StringSerializer.class.getName());
//        properties.put("key.serializer",org.apache.kafka.common.serialization.StringSerializer.class.getName());
//        properties.put("value.serializer",org.apache.kafka.common.serialization.StringSerializer.class.getName());
//        properties.put("request.required.acks","1");


//        创建生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.43.102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //优化参数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);//生产者尝试缓存记录，为每一个分区缓存一个mb的数据
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);//最多等待0.5秒.

        //允许超时最大时间
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,5000);
        //失败尝试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);



        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        while (true){



            Record re=new Record(sdf.format(new Date()),i+"","test01");
            i++;

            String format = JSONArray.toJSON(re).toString();
            System.out.println(format);



            ProducerRecord<String, String> record = new ProducerRecord<>("topic1",  format);
             Future<RecordMetadata> send = kafkaProducer.send(record);
            System.out.println(send.get().toString()+" --> offset:"+send.get().offset());


//            System.out.println(send.isDone());

//            TimeUnit.MILLISECONDS.sleep(100);//毫秒
        }


    }




}
