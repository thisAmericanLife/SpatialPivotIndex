package usace.army.mil.erdc.pivots.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import usace.army.mil.erdc.pivots.accumulo.AccumuloConnectionManager;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

public class PivotTopology {
	private static final String JAR_PATH = "/home/ktyler/Documents/strider/Pivots/target/Pivots-0.0.1-SNAPSHOT.jar";
	private static final String INSTANCE_NAME = "strider";
	private static final String ZOOKEEPERS = "schweinsteiger:2181,neuer:2181,neymar:2181";
	private static final String ACCUMULO_USER = "root";
	private static final String ACCUMULO_PASSWORD = "amledd";
	final private static Gson gson = new Gson();

	public static void main(String [] args){
		initTopology();
	}

	private static ImmutableList<String> getZooKeeperHosts(){
		return ImmutableList.of("schweinsteiger", "neuer", "neymar");
	}

	private static String getZooKeeperHostsString(){
		return "schweinsteiger,neuer,neymar"; 
	}

	private static void initTopology(){
		System.setProperty("storm.jar", JAR_PATH);
		String topic = "pivot_points";


		Map defaultConf = Utils.readStormConfig();
		Map conf1 = new HashMap();                     
		conf1.put(Config.TOPOLOGY_WORKERS, 16);                
		conf1.put(Config.STORM_ZOOKEEPER_SERVERS, getZooKeeperHosts());
		conf1.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf1.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 10000);
		conf1.put(Config.NIMBUS_HOST, "chicharito");
		conf1.put(Config.NIMBUS_THRIFT_PORT, 6629);
		//conf1.put(Config.NIMBUS_THRIFT_PORT, defaultConf.get(Config.NIMBUS_THRIFT_PORT));
		conf1.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, defaultConf.get(Config.STORM_THRIFT_TRANSPORT_PLUGIN));
		//conf1.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m -Djava.net.preferIPv4Stack=true");
		//conf1.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2048m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m");
		conf1.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2048m -XX:MaxPermSize=256m -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -verbose:gc -Xloggc:/opt/apache-storm-0.9.5/logs/gc-storm-worker-%ID%.log");
		conf1.put(Config.NIMBUS_TASK_TIMEOUT_SECS,300);
		conf1.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,60);
		conf1.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT,30000);
		conf1.put(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS,3000);
		conf1.put(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS,1000);

		Connector connector = getConnector();  LocalCluster cluster = new LocalCluster();
        
		try {                              
		//	cluster.submitTopology(topic, conf1, createTopology(topic, pathToDrobo));
			StormSubmitter.submitTopology(topic, conf1, createTopology(topic, connector));
		} catch (AlreadyAliveException | InvalidTopologyException e1) {
			e1.printStackTrace();
		}               
	}
	
	private static Point getQueryPoint(){
		Point point = new Point();
		point.setUID("point_9165");
		point.setX(37.511528);
		point.setY(-122.342438);
		return point;
	}
	
	private static double getPrecomputedDistanceFromAccumulo(Point point, Pivot pivot, Connector connector){
		double distance = 0.0;
		Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter("pointsIndex", 
				pivot.getPivotID(),
				point.getUID(), "PIVOT", connector);
		for(Entry<Key,Value> scannerEntry : scanner) {
				distance = Double.parseDouble(scannerEntry.getKey().getColumnQualifier().toString());
				break;
		}
		return distance;
	}
	
	private static Map<String,Double> getPivotMap(List<Pivot> pivots, Point queryPoint, Connector connector){
		Map<String,Double> pivotDistancesToQueryPoint = new HashMap<String,Double>();
		for(Pivot pivot: pivots){
			pivotDistancesToQueryPoint.put(pivot.getPivotID(), 
					getPrecomputedDistanceFromAccumulo(queryPoint, pivot, connector));
		}
		return pivotDistancesToQueryPoint;
	}
	
	private static List<Pivot> getPivots(Connector connector){
		Scanner pivots = AccumuloConnectionManager.queryAccumulo("points", "PIVOT", "POJO", connector);
		List<Pivot> pivotList = new ArrayList<Pivot>();
		for(Entry<Key,Value> entrySet : pivots){
			pivotList.add(gson.fromJson(entrySet.getValue().toString(), Pivot.class));
		}
		return pivotList;
	}

	private static Connector getConnector(){
		Instance instance  = new ZooKeeperInstance(INSTANCE_NAME, ZOOKEEPERS);
		Connector connector = null;
		try {
			connector = instance.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
		} catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}
		return connector;
	}

	private static StormTopology createTopology(String topic, Connector connector)
	{
		//Pivot-specific variables
		double range = .4;
		Point queryPoint = getQueryPoint();
		List<Pivot> pivots = getPivots(connector);
		Map<String,Double> pivotMap = getPivotMap(pivots, queryPoint, connector);
		//Storm and Kafka variables 
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(getZooKeeperHostsString()), topic, "/opt/zookeeper-3.4.6","Pivot-Kafka-Spout");
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConf.forceFromStart=true;
		
		TopologyBuilder builder = new TopologyBuilder();         
		builder.setSpout("point_spout", new KafkaSpout(kafkaConf),4);
		builder.setBolt("range_query", new PivotRangeQueryBolt(), 24).shuffleGrouping("point_spout");
		builder.setBolt("filter_bolt", new PivotFilterBolt(pivots, pivotMap, connector, range), 64).shuffleGrouping("range_query");
		builder.setBolt("refine_bolt", new PivotRefineBolt(queryPoint, range),24).shuffleGrouping("filter_bolt");

		/*  builder.setBolt("twitter_parser", new TwitterParserBolt(),24).shuffleGrouping("twitter_spout");          
         builder.setBolt("tweet_to_drobo", new TweetToDrobo(), 1).shuffleGrouping("twitter_parser");             
         builder.setBolt("tweet-to-accumulo", new TweetToAccumulo(), 64).shuffleGrouping("twitter_parser");               
         builder.setBolt("twitter-concept-to-accumulo", new TweetConceptToAccumulo(), 64).shuffleGrouping("tweet-to-accumulo");*/

		return builder.createTopology();
	}  
}
