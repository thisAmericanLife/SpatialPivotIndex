package usace.army.mil.erdc.pivots.storm.knn;

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

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import usace.army.mil.erdc.Pivots.models.Quadrant;
import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.accumulo.AccumuloConnectionManager;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.storm.PivotFilterBolt;
import usace.army.mil.erdc.pivots.storm.PivotRangeQueryBolt;
import usace.army.mil.erdc.pivots.storm.PivotRefineBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

public class PivotKnnTopology {
	private static final String JAR_PATH = "/home/ktyler/Documents/strider/Pivots/target/Pivots-0.0.1-SNAPSHOT.jar";
	private static final String INSTANCE_NAME = "strider";
	private static final String ZOOKEEPERS = "schweinsteiger:2181,neuer:2181,neymar:2181";
	private static final String ACCUMULO_USER = "root";
	private static final String ACCUMULO_PASSWORD = "amledd";
	final private static Gson gson = new Gson();
	
	private static int NUM_NEIGHBORS_FOUND = 0;
	private static int NUM_NEIGHBORS_TO_FIND = 0;
	private static double RANGE = 0.0;
	private static List<Pivot> PIVOTS;
	private static Map<String,Double> PIVOT_MAP;
	private static Point QUERY_POINT;

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

		Connector connector = getConnector();  
		LocalCluster cluster = new LocalCluster();
        
		try {                              
			cluster.submitTopology(topic, conf1, createTopology(topic,  connector));
			//StormSubmitter.submitTopology(topic, conf1, createTopology(topic, connector));
		} catch (Exception e1) {
			e1.printStackTrace();
		}               
	}
	
	private static Point getPoint(){
		Point point = new Point();
		point.setUID("point_1000011");
		point.setX(40.190331);
		point.setY(-82.669468);
		return point;
	}
	
	private static double getPrecomputedDistanceFromAccumulo(Point point, Pivot pivot, Connector connector){
		double distance = 0.0;
		Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter("points", 
				pivot.getPivotID(),
				point.getUID(), "PIVOT", connector);
		for(Entry<Key,Value> scannerEntry : scanner) {
				distance = Double.parseDouble(scannerEntry.getKey().getColumnQualifier().toString());
				break;
		}
		return distance;
	}
	
	private static Map<String,Double> getPivotMap(Connector connector){
		Map<String,Double> pivotDistancesToQueryPoint = new HashMap<String,Double>();
		for(Pivot pivot: PIVOTS){
			pivotDistancesToQueryPoint.put(pivot.getPivotID(), 
					getPrecomputedDistanceFromAccumulo(QUERY_POINT, pivot, connector));
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
	
	private static Quadrant getQuadrant(String cf, String cq, Connector connector){
		Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter("points", 
				"!!!QUADRANT", cf, cq, connector);
		Quadrant quadrant = null;
		for(Entry<Key,Value> scannerEntry : scanner) {
			quadrant = gson.fromJson(scannerEntry.getValue().toString(), Quadrant.class);
				break;
		}
		return quadrant;
	}
	
	private static String getQuadrant(Point point, Point centroid){
		double x = point.getX();
		double y = point.getY();
		
		if(x <= centroid.getX() && y >= centroid.getY()){
			return "UPPER LEFT";
		} else if(x >= centroid.getX() && y >= centroid.getY()){
			return "UPPER RIGHT";
		} else if(x >= centroid.getX() && y <= centroid.getY()){
			return "LOWER RIGHT";
		} else{
			return "LOWER LEFT";
		}
	}
	
	private static Point getCentroid(Connector connector){
		Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter("points", 
				"!!!QUADRANT", "CENTROID", "POINT", connector);
		Point centroid = null;
		for(Entry<Key,Value> scannerEntry : scanner) {
			centroid = gson.fromJson(scannerEntry.getValue().toString(), Point.class);
				break;
		}
		return centroid;
	}
	
	private static double getInitialRange(Connector connector){
		Point centroid = getCentroid(connector);
		String [] delimitedColumns = getQuadrant(QUERY_POINT, centroid).split(" ");
		return PivotIndex.deriveRange(NUM_NEIGHBORS_TO_FIND, 
				getQuadrant(delimitedColumns[0], delimitedColumns[1], connector).getDensity());
	}

	private static StormTopology createTopology(String topic, Connector connector)
	{
		//Pivot-specific variables
		PIVOTS = getPivots(connector);
		NUM_NEIGHBORS_TO_FIND = 5;
		QUERY_POINT = getPoint();
		PIVOT_MAP = getPivotMap(connector);
		RANGE = getInitialRange(connector);
		System.out.println("Beginning query with initial range: " + RANGE);
		//Storm and Kafka variables 
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(getZooKeeperHostsString()), topic, "/opt/zookeeper-3.4.6","Pivot-Kafka-Spout");
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConf.forceFromStart=true;
		
		TopologyBuilder builder = new TopologyBuilder();         
		builder.setSpout("point_spout", new KafkaSpout(kafkaConf),4);
		builder.setBolt("filter_bolt", new PivotKnnFilterBolt(), 24).shuffleGrouping("point_spout").shuffleGrouping("refine_bolt");
		builder.setBolt("refine_bolt", new PivotKnnRefineBolt(), 24).shuffleGrouping("filter_bolt");
		//builder.setBolt("filter_bolt", new PivotFilterBolt(pivots, pivotMap, connector, range), 64).shuffleGrouping("range_query");
		//builder.setBolt("refine_bolt", new PivotRefineBolt(queryPoint, range),12).shuffleGrouping("filter_bolt");

		return builder.createTopology();
	}

	
	//Getters and setters
	public static int getNumNeighborsFound() {
		return NUM_NEIGHBORS_FOUND;
	}
	public static void incrementNumNeighborsFound() {
		NUM_NEIGHBORS_FOUND++;
	}  
	public static double getRange() {
		return RANGE;
	}
	public static List<Pivot> getPivots() {
		return PIVOTS;
	}
	public static Map<String,Double> getPivotMap() {
		return PIVOT_MAP;
	}
	public static Point getQueryPoint() {
		return QUERY_POINT;
	}
	public static int getNumNeighborsToFind() {
		return NUM_NEIGHBORS_TO_FIND;
	}

}
