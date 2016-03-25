package usace.army.mil.erdc.pivots.kafka;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerManager {
	private static final String METADATA_BROKER_LIST = "schweinsteiger:9092,neuer:9092";
	Map<String, Object> configs;
	KafkaProducer<byte[], byte[]> producer = null;
	public KafkaProducer<byte[], byte[]> getProducer() {    
		if(this.producer == null){
			init();
		}
		return this.producer;
	}

	public ProducerManager(){              
	}


	@PostConstruct
	public void init() {
		configs = new HashMap<String, Object>();
		configs.put("bootstrap.servers", METADATA_BROKER_LIST); //do not add more than 1 broker
		configs.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		configs.put("acks", "0");
		this.producer = new KafkaProducer<byte[], byte[]>(configs); 
	}

}
