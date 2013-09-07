package storm.kafka.trident;

import storm.kafka.KafkaConfig;


public class TridentKafkaConfig extends KafkaConfig {
    public TridentKafkaConfig(BrokerHosts hosts, String topic) {
        super(hosts, topic);
    }
    
    public IBatchCoordinator coordinator = new DefaultCoordinator();
    
    // if true, re-submitted batch ids (i.e. with attempts ids > 0) to a Trident spout will be mentioned in the logs at info level
    public boolean traceReAttempts = false;

}
