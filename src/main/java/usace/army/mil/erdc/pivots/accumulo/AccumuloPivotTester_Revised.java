package usace.army.mil.erdc.pivots.accumulo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Properties;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.PivotTester;
import usace.army.mil.erdc.pivots.models.IIndexingScheme;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.PivotIndexFactory;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class AccumuloPivotTester_Revised extends PivotTester {
	final private static Gson gson = new Gson();
	private final static String CALIFORNIA_ROADS_PATH = "/home/hduser/CaliforniaRoadNetworksNodes.txt";
	private final static String WALKING_DEAD_TWEETS_PATH = "/home/hduser/twitter_sm.tsv";
	




	//private static ClientOnRequiredTable opts = null;
	private static AccumuloConnectionManager connectionManager = null;
	private static ClientOnRequiredTable opts = null;
	private static BatchWriterOpts bwOpts = null;
	private static BatchWriterConfig bwConfig = null;
	
	private static Point selectPointFromListRandomly(List<Point> points){
		Random random = new Random();
		return points.get(random.nextInt(10000));
	}

	private static Point selectPointFromListRandomly(Scanner points, int datasetSize){
		/*Random random = new Random();
		Scanner randomPoint = AccumuloConnectionManager.queryAccumulo("points",
				"point_" + random.nextInt(datasetSize), "POINT", "POJO");
		Point point = null;
		for(Entry<Key,Value> entrySet : randomPoint){
			point = gson.fromJson(entrySet.getValue().toString(), Point.class);
		}*/
		Point randomPoint = new Point();
		randomPoint.setUID("point_9165");
		randomPoint.setX(37.511528);
		randomPoint.setY(-122.342438);
		return randomPoint;
	}
	
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
					String UID = PivotUtilities.hashUID("point_" + i);
					
					point.setUID(UID);
					mutations.add(AccumuloConnectionManager.getMutation(UID, "POINT", "POJO", gson.toJson(point, Point.class)));
					//Write to Accumulo
				} else {
					String [] delimitedString = line.split("\t");
					point.setX(Double.parseDouble(delimitedString[6]));
					point.setY(Double.parseDouble(delimitedString[5]));
					String UID = PivotUtilities.hashUID("point_" + i);
					
					point.setUID(UID);
					mutations.add(AccumuloConnectionManager.getMutation(UID, "POINT", "POJO", gson.toJson(point, Point.class)));
					//Write to Accumulo
				}
				
				i++;
				/*batchWriterIndex++;
				//Flush every 500 values
				if(batchWriterIndex > 499){
					AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
					mutations.clear();
					batchWriterIndex = 0;
				}*/
			}
			AccumuloConnectionManager.writeMutations(mutations,  bwOpts, bwConfig, false);
			List<Mutation> mutationList = new ArrayList<Mutation>();
			mutationList.add(AccumuloConnectionManager.getMutation("!!!POINT_COUNT", "DATASET", "COUNT", String.valueOf(i+ 1)));
			AccumuloConnectionManager.writeMutations(mutationList,  bwOpts, bwConfig, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
	}

	private static List<Point> populatePointsFromDisk(String filename){
		List<Point> points = new ArrayList<Point>();
		PointFactory pointFactory = new PointFactory();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			int i = 0;
			while ((line = br.readLine()) != null) {
				if(! filename.substring(filename.length() -4).equals(".tsv")){
					Point point = (Point)pointFactory.getPoint(IPoint.PointType.POINT);
					point.setX(Double.parseDouble(Arrays.asList(line.split(" ")).get(2)));
					point.setY(Double.parseDouble(Arrays.asList(line.split(" ")).get(1)));
					point.setUID(String.valueOf(i));
					points.add(point);
				} else{
					String [] delimitedString = line.split("\t");
					Point point = (Point)pointFactory.getPoint(IPoint.PointType.POINT);
					point.setX(Double.parseDouble(delimitedString[6]));
					point.setY(Double.parseDouble(delimitedString[5]));
					point.setUID(String.valueOf(i));
					points.add((Point)point);
				}

			}
		}catch (IOException ex) {
			ex.printStackTrace();
			System.exit(0);
		} 
		return points;
	}

	public static List<Point> populatePointsFromCaliforniaRoadsDataset(){
		List<Point> points = new ArrayList<Point>();

		try (BufferedReader br = new BufferedReader(new FileReader(CALIFORNIA_ROADS_PATH))) {
			String line;
			while ((line = br.readLine()) != null) {
				points.add(new Point(Double.parseDouble(Arrays.asList(line.split(" ")).get(1)),
						Double.parseDouble(Arrays.asList(line.split(" ")).get(2))));

			}
		}catch (IOException ex) {
			ex.printStackTrace();
			System.exit(0);
		} 
		return points;
	}
	
	private static List<Point> getPoints(Scanner scanner){
		List<Point> points = new ArrayList<Point>();
		for(Entry<Key,Value> pointEntry : scanner) {
			points.add(gson.fromJson(pointEntry.getValue().toString(), Point.class));
		}
		return points;
	}

	private  static void populatePointsInAccumulo(List<Point> points){
		List<Mutation> mutations = new ArrayList<Mutation>();
		//Populate random points
		int i = 0;
		for(Point point : points){
			mutations.add(AccumuloConnectionManager.getMutation("point_" + i, "POINT", "POJO", gson.toJson(point, Point.class)));
			i++;
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig, false);
	}

	private static void init(String [] args) throws FileNotFoundException{
		opts = new ClientOnRequiredTable();
		bwOpts = new BatchWriterOpts();
		bwConfig = bwOpts.getBatchWriterConfig();
		opts.parseArgs(AccumuloPivotTester_Revised.class.getName(), args, bwOpts, bwConfig);

		connectionManager = new AccumuloConnectionManager(opts);
		connectionManager.connect();
		//Verify table exists
		AccumuloConnectionManager.verifyTableExistence(opts.getTableName());
		AccumuloConnectionManager.verifyTableExistence("pointsIndex");

		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IIndexingScheme index = indexFactory.getIndex(IIndexingScheme.PivotIndexType.ACCUMULO);
		
		
		Scanner pointsScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		List<Point> points = getPoints(pointsScanner);
		//BatchScanner points = AccumuloConnectionManager.getBatchScanner("points");
		
		
		System.out.println("Selecting pivots.");
		long startTime = System.currentTimeMillis();
		List<Pivot> pivots = ((PivotIndex)index).choosePivotsSparseSpatialIndex(points,true);
		long pivotSelectionTime = System.currentTimeMillis();
		System.out.println("Time take to select pivots: " + (pivotSelectionTime - startTime));
		
		//Get map values
		System.out.println("Mapping pivots...");
	
		long beginPivotMapTime = System.currentTimeMillis();
		((PivotIndex)index).populatePivotMapValues(pivots, points);
		long endPivotMapTime = System.currentTimeMillis();
		System.out.println("Time taken to map pivots: " + (endPivotMapTime - beginPivotMapTime));
		
		
		//Get randomly selected point from newly created points
		Point queryPoint = selectPointFromListRandomly(points);
		System.out.println("Query point: " + queryPoint.getX() + ", " + queryPoint.getY() + ".");

		//Get distances map
		Map<Double, Pivot> distanceMap = PivotIndex.getDistanceMap(points, pivots, queryPoint);
		
		//Issue range query
		System.out.println("Performing range query...");
		long rangeQueryBeforeTime = System.currentTimeMillis();
		
		
		
		
		List<Point> nearestNeighbors = ((PivotIndex)index).rangeQuery(points, pivots,queryPoint, distanceMap, 2.5);
		long rangeQueryAfterTime = System.currentTimeMillis();
		System.out.println("Time taken to perform range query: " + (rangeQueryAfterTime - rangeQueryBeforeTime) + " milliseconds." );
		int neighborCounter = 0;
		for(Point neighbor: nearestNeighbors){
			System.out.println("Neighbor: " + neighbor.getX() + ", " + neighbor.getY() + 
					".  Distance from query point: " + PivotUtilities.getDistance(queryPoint, neighbor) +
					".");
			if(nearestNeighbors.indexOf(neighbor) > 9){
				break;
			}
		}
	}

	public static void main(String [] args){
		if(args.length != 10){
			System.out.println("Usage: bin/accumulo jar lib/Pivots-0.0.1-SNAPSHOT.jar "
					+ "usace.army.mil.erdc.pivots.accumulo.AccumuloPivotTester -t <tablename> -i <instance> -u <user> -p <password "
					+ "--keepers <zookeeper host:port>");
			System.exit(0);
		} else{
			try {
				init(args);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
