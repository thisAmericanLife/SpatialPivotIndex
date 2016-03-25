package usace.army.mil.erdc.pivots.storm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import usace.army.mil.erdc.pivots.accumulo.AccumuloConnectionManager;
import usace.army.mil.erdc.pivots.accumulo.AccumuloPivotIndex;
import usace.army.mil.erdc.pivots.accumulo.AccumuloPivotTester;
import usace.army.mil.erdc.pivots.models.IIndexingScheme;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.PivotIndexFactory;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

public class KafkaImporter {
	final private static Gson gson = new Gson();
	private static AccumuloConnectionManager connectionManager = null;
	private static ClientOnRequiredTable opts = null;
	private static BatchWriterOpts bwOpts = null;
	private static BatchWriterConfig bwConfig = null;
	private static final String METADATA_BROKER_LIST = "schweinsteiger:9092,neuer:9092";
	private static final String TOPIC_NAME = "pivot_points";
	
	private static String getValueFromConfigFile(String type) throws FileNotFoundException{
		InputStream inputStream;
		Properties prop = new Properties();
		String propFileName = "/home/hduser/pivots.properties";
		inputStream = new FileInputStream(propFileName);
		
		
		if (inputStream != null) {
			try {
				
				prop.load(inputStream);
				if(type.equals("dataset")){
					return prop.getProperty("dataset");
				} else{
					return prop.getProperty("range");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			
		}
		System.exit(0);

		return null;
	}
	
	private static BufferedReader getBufferedReader(String filename){
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return br;
	}
	
	private static Scanner populateAccumuloFromDisk(String filename){
		PointFactory pointFactory = new PointFactory();
		
		BufferedReader br = getBufferedReader(filename);
		boolean isWalkingDeadData = false;
		if(! filename.substring(filename.length() -4).equals(".tsv")){
			isWalkingDeadData = true;
		}
		String line;
		int i = 0;
		int batchWriterIndex = 0;
		List<Mutation> mutations = new ArrayList<Mutation>();
		try {
			while((line = br.readLine()) != null){
				
				Point point = (Point)pointFactory.getPoint(IPoint.PointType.POINT);
				if(isWalkingDeadData){
					point.setX(Double.parseDouble(Arrays.asList(line.split(" ")).get(2)));
					point.setY(Double.parseDouble(Arrays.asList(line.split(" ")).get(1)));
					String UID = "point_" + i;
					
					point.setUID(UID);
					mutations.add(AccumuloConnectionManager.getMutation(UID, "POINT", "POJO", gson.toJson(point, Point.class)));
					//Write to Accumulo
				} else {
					String [] delimitedString = line.split("\t");
					point.setX(Double.parseDouble(delimitedString[6]));
					point.setY(Double.parseDouble(delimitedString[5]));
					String UID = "point_" + i;
					
					point.setUID(UID);
					mutations.add(AccumuloConnectionManager.getMutation(UID, "POINT", "POJO", gson.toJson(point, Point.class)));
					//Write to Accumulo
				}
				
				i++;
				batchWriterIndex++;
				//Flush every 500 values
				if(batchWriterIndex > 50000){
					AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
					mutations.clear();
					batchWriterIndex = 0;
				}
			}
			mutations.add(AccumuloConnectionManager.getMutation("!!!POINT_COUNT", "DATASET", "COUNT", String.valueOf(i+ 1)));
			if(batchWriterIndex > 0){
				AccumuloConnectionManager.writeMutations(mutations,  bwOpts, bwConfig, false);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
	}
	
	private static void sendPointsToKafka(Scanner points, String topicName){
		Properties props = new Properties();
		 
		props.put("metadata.broker.list", METADATA_BROKER_LIST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		 
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		
		
		for(Entry<Key,Value> scannerEntry : points) {
			KeyedMessage<String, String> data = 
					new KeyedMessage<String, String>(topicName, scannerEntry.getValue().toString());
   		 
    		producer.send(data);
		}
	}
	
	
	
	public static void main(String [] args){
		//Initialize global Accumulo variables
		opts = new ClientOnRequiredTable();
		bwOpts = new BatchWriterOpts();
		bwConfig = bwOpts.getBatchWriterConfig();
		opts.parseArgs(AccumuloPivotTester.class.getName(), args, bwOpts, bwConfig);
		connectionManager = new AccumuloConnectionManager(opts);
		connectionManager.connect();
		
		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IIndexingScheme index = indexFactory.getIndex(IIndexingScheme.PivotIndexType.ACCUMULO);
		
		//Verify table exists
		AccumuloConnectionManager.prepareTablesForTest(ImmutableList.of("pivots", "pivotsIndex"));
		AccumuloConnectionManager.verifyTableExistence(opts.getTableName());
		
		try {
			//Load into Accumulo
			populateAccumuloFromDisk(getValueFromConfigFile("dataset"));
			//Get points
			Scanner points = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
			//Get pivots
			List<Pivot> pivots = ((AccumuloPivotIndex)index).choosePivotsSparseSpatialIndex(points, bwConfig, true);
			//Populate pivot map values into pivot index table
			((AccumuloPivotIndex)index).populatePivotMapValues(pivots, points, bwConfig);
			//Load into kafka topic, "pivot_points"
			sendPointsToKafka(points, TOPIC_NAME);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--instance")
		String instance = "strider";
		@Parameter(names = "--zookeepers")
		String zookeepers = "schweinsteiger:2181";
		@Parameter(names = "--username")
		String username = "root";
		@Parameter(names = "--password")
		String password = "amledd";
		@Parameter(names = "--table")
		String table = "tweets";
	}
}
