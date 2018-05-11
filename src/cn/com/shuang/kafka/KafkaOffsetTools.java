package cn.com.shuang.kafka;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;  
  
public class KafkaOffsetTools {  
	
	private static KafkaConsumer<String, String> kc;
  
    public static void main(String[] args) {  
    	KafkaOffsetTools kot = new KafkaOffsetTools(); 
//    	kot.readOffset();
    	String topic = "spark_streaming_alarm_topic";
    	kot.readOffsetByKafkaClient(topic);
    }  
    
    
    public void readOffsetByKafkaClient(String topic){
    	KafkaConsumer<String, String> kc = getConsumer();
    	TopicPartition partition0 = new TopicPartition(topic, 0);
    	TopicPartition partition1 = new TopicPartition(topic, 1);
//    	kc.subscribe(Arrays.asList(topic));    
//    	kc.subscribe(Collections.singletonList(topic));
    	
//    	kc.seekToBeginning(new ArrayList<TopicPartition>());
//    	Map<String, List<PartitionInfo>> map = kc.listTopics();
//    	for(Entry<String, List<PartitionInfo>> pi :map.entrySet()){
//    		System.out.println("Key = " + pi.getKey() + ", Value = " + pi.getValue());  
//    	}
//    	System.out.println("map :"+map.get(topic));
//    	Map<TopicPartition, Long> map2 = kc.beginningOffsets(Arrays.asList(new TopicPartition(topic, 0)));
//    	System.out.println("map2 :"+map2.get(new TopicPartition(topic, 0)));
    	
    	kc.assign(Arrays.asList(partition0));	//订阅模式
    	Long current_offset = kc.position(new TopicPartition(topic, 0));
    	System.out.println("topic :"+topic+" current_offset :"+current_offset);
    }
    public static KafkaConsumer<String, String> getConsumer() {    
        if(kc == null) {    
            Properties props = new Properties();    
              
            props.put("bootstrap.servers", "135.251.208.66:9092,135.251.208.67:9092,135.251.208.68:9092");    
            props.put("group.id", "alarmBasedata");    
            props.put("enable.auto.commit", "false");    
            props.put("auto.commit.interval.ms", "7000");    
            props.put("session.timeout.ms", "30000");    
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");    
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");    
            kc = new KafkaConsumer<String, String>(props);    
        }    
          
        return kc;    
    } 
    

    public void readOffset(){
    	// 读取kafka最新数据  
        // Properties props = new Properties();  
        // props.put("zookeeper.connect",  
        // "192.168.6.18:2181,192.168.6.20:2181,192.168.6.44:2181,192.168.6.237:2181,192.168.6.238:2181/kafka-zk");  
        // props.put("zk.connectiontimeout.ms", "1000000");  
        // props.put("group.id", "dirk_group");  
        //  
        // ConsumerConfig consumerConfig = new ConsumerConfig(props);  
        // ConsumerConnector connector =  
        // Consumer.createJavaConsumerConnector(consumerConfig);  
  
        String topic = "spark_streaming_test_topic";  
        String seed = "135.251.208.67";  
        int port = 9092;  
        List<String> seeds = new ArrayList<String>();  
        seeds.add("135.251.208.66");
        seeds.add("135.251.208.67"); 
        seeds.add("135.251.208.68"); 
         
  
        TreeMap<Integer,PartitionMetadata> metadatas = findLeader(seeds, port, topic);  
          
        int sum = 0;  
        int sumOfconsumers = 0;    
        for (Entry<Integer,PartitionMetadata> entry : metadatas.entrySet()) {  
            int partition = entry.getKey();  
            String leadBroker = entry.getValue().leader().host();  
            String clientName = "Client_" + topic + "_" + partition;  
            SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000,  
                    64 * 1024, clientName);  
            long readLastOffset = getLastOffset(consumer, topic, partition,  
                    kafka.api.OffsetRequest.LatestTime(), clientName);  
            long readEarlyOffset = getEarlyOffset(consumer, topic, partition,  
                    kafka.api.OffsetRequest.EarliestTime(), clientName); 
            sum += readLastOffset;  
            sumOfconsumers += readLastOffset-readEarlyOffset;
            System.out.println("readEarlyOffset :"+partition+":"+readEarlyOffset); 
            System.out.println("readLastOffset :"+partition+":"+readLastOffset);  
            if(consumer!=null)consumer.close();  
        }  
        System.out.println("总和："+sum);  
        System.out.println("可消费："+sumOfconsumers);  
    }
  
    public static long getLastOffset(SimpleConsumer consumer, String topic,  
            int partition, long whichTime, String clientName) {  
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,  
                partition);  
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();  
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(  
                whichTime, 1));  
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(  
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),  
                clientName);  
        OffsetResponse response = consumer.getOffsetsBefore(request);  
  
        if (response.hasError()) {  
            System.out  
                    .println("Error fetching data Offset Data the Broker. Reason: "  
                            + response.errorCode(topic, partition));  
            return 0;  
        }  
        long[] offsets = response.offsets(topic, partition);  
//      long[] offsets2 = response.offsets(topic, 3);  
        return offsets[0];  
    }  
    
    public static long getEarlyOffset(SimpleConsumer consumer, String topic,  
            int partition, long whichTime, String clientName) {  
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic,  
                partition);  
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();  
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(  
                whichTime, 1));  
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(  
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(),  
                clientName);  
        OffsetResponse response = consumer.getOffsetsBefore(request);  
  
        if (response.hasError()) {  
            System.out  
                    .println("Error fetching data Offset Data the Broker. Reason: "  
                            + response.errorCode(topic, partition));  
            return 0;  
        }  
        long[] offsets = response.offsets(topic, partition);  
//      long[] offsets2 = response.offsets(topic, 3);  
        return offsets[0];  
    }
  
    private TreeMap<Integer,PartitionMetadata> findLeader(List<String> a_seedBrokers,  
            int a_port, String a_topic) {  
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();  
        loop: for (String seed : a_seedBrokers) {  
            SimpleConsumer consumer = null;  
            try {  
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,  
                        "leaderLookup"+new Date().getTime());  
                List<String> topics = Collections.singletonList(a_topic);  
                TopicMetadataRequest req = new TopicMetadataRequest(topics);  
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);  
  
                List<TopicMetadata> metaData = resp.topicsMetadata();  
                for (TopicMetadata item : metaData) {  
                    for (PartitionMetadata part : item.partitionsMetadata()) {  
                        map.put(part.partitionId(), part);  
//                      if (part.partitionId() == a_partition) {  
//                          returnMetaData = part;  
//                          break loop;  
//                      }  
                    }  
                }  
            } catch (Exception e) {  
                System.out.println("Error communicating with Broker [" + seed  
                        + "] to find Leader for [" + a_topic + ", ] Reason: " + e);  
            } finally {  
                if (consumer != null)  
                    consumer.close();  
            }  
        }  
//      if (returnMetaData != null) {  
//          m_replicaBrokers.clear();  
//          for (kafka.cluster.Broker replica : returnMetaData.replicas()) {  
//              m_replicaBrokers.add(replica.host());  
//          }  
//      }  
        return map;  
    }  
  
}