package storm.kafka.trident;

import backtype.storm.utils.Utils;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.common.OffsetOutOfRangeException;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

public class KafkaUtils {
    
	public static final Logger LOG = Logger.getLogger(KafkaUtils.class);

    
     public static Map emitPartitionBatchNew(TridentKafkaConfig config, int partition, SimpleConsumer consumer, TransactionAttempt attempt, TridentCollector collector, Map lastMeta, String topologyInstanceId) {
    	 
    	 traceReattempt(config, attempt);
    	 
         StaticHosts hosts = (StaticHosts) config.hosts;
         long offset;
         if(lastMeta!=null) {
             if(config.forceFromStart && !topologyInstanceId.equals(lastMeta.get("instanceId"))) {
                 offset = consumer.getOffsetsBefore(config.topic, partition % hosts.partitionsPerHost, config.startOffsetTime, 1)[0];
             } else {
                 offset = (Long) lastMeta.get("nextOffset");                 
             }
         } else {
             long startTime = -1;
             if(config.forceFromStart) startTime = config.startOffsetTime;
             offset = consumer.getOffsetsBefore(config.topic, partition % hosts.partitionsPerHost, startTime, 1)[0];
         }
         ByteBufferMessageSet msgs;
         try {
            msgs = consumer.fetch(new FetchRequest(config.topic, partition % hosts.partitionsPerHost, offset, config.fetchSizeBytes));
         } catch(Exception e) {
             if(e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
         }
         long endoffset = offset;
	         for(MessageAndOffset msg: msgs) {
	             emit(config, attempt, collector, msg.message());
	             endoffset = msg.offset();
	         }
         Map newMeta = new HashMap();
         newMeta.put("offset", offset);
         newMeta.put("nextOffset", endoffset);
         newMeta.put("instanceId", topologyInstanceId);
         return newMeta;
     }
     
     public static void emit(TridentKafkaConfig config, TransactionAttempt attempt, TridentCollector collector, Message msg) {
         List<Object> values = config.scheme.deserialize(Utils.toByteArray(msg.payload()));
         if(values!=null) {
             collector.emit(values);
         }
     }
     
     
     private static void traceReattempt(TridentKafkaConfig config, TransactionAttempt attempt) {
    	 if (config != null && config.traceReAttempts && attempt != null && attempt.getAttemptId() > 0) {
    		 LOG.info(String.format("Re-attempting transaction id %d, attempt id is %d", attempt.getTransactionId(), attempt.getAttemptId()));
    	 }
     }
     
}
