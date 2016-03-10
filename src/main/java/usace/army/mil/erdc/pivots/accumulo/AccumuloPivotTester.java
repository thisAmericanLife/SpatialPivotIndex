package usace.army.mil.erdc.pivots.accumulo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Properties;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;
import com.google.gson.Gson;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.PivotTester;
import usace.army.mil.erdc.pivots.models.IPivotIndex;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.PivotIndexFactory;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class AccumuloPivotTester extends PivotTester {
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
	
	private String getFileName(String propertyFileName){
		InputStream inputStream;
		Properties prop = new Properties();
		String propFileName = "config.properties";

		inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return prop.getProperty("dataset");
			}
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		}

		

		// get the property value and print it out
		String user = prop.getProperty("user");

		return null;
	}

	private static List<Point> populatePointsFromDisk(String filename){
		List<Point> points = new ArrayList<Point>();

		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
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

	private static List<Point> populatePointsFromCaliforniaRoadsDataset(){
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

	private  static void populatePointsInAccumulo(List<Point> points){
		List<Mutation> mutations = new ArrayList<Mutation>();
		//Populate random points
		int i = 0;
		for(Point point : points){
			mutations.add(AccumuloConnectionManager.getMutation("point_" + i, "POINT", "POJO", gson.toJson(point, Point.class)));
			i++;
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
	}

	private static void init(String [] args){
		opts = new ClientOnRequiredTable();
		bwOpts = new BatchWriterOpts();
		bwConfig = bwOpts.getBatchWriterConfig();
		opts.parseArgs(AccumuloPivotTester.class.getName(), args, bwOpts, bwConfig);

		connectionManager = new AccumuloConnectionManager(opts);
		connectionManager.connect();
		//Verify table exists
		connectionManager.verifyTableExistence(opts.getTableName());

		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IPivotIndex index = indexFactory.getIndex(IPivotIndex.PivotIndexType.ACCUMULO);
		List<Point> points = populatePointsFromDisk(args[10]);
		//Get points
		populatePointsInAccumulo(points);
		//Calculate pivots
		System.out.println("Selecting pivots.");
		long startTime = System.currentTimeMillis();
		List<Pivot> pivots = index.choosePivotsSparseSpatialIndex(points, true);
		long pivotSelectionTime = System.currentTimeMillis();
		System.out.println("Time take to select pivots: " + (pivotSelectionTime - startTime));
		//Get map vaules
		System.out.println("Mapping pivots...");
		pivots = index.populatePivotMapValues(pivots, points);
		long pivotMapTime = System.currentTimeMillis();
		//Write pivot values to accumulo
		Scanner pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		((AccumuloPivotIndex)index).writePivotsToAccumulo(pivots, pointScanner, bwConfig);

		//Get randomly selected point from newly created points
		Point queryPoint = selectPointFromListRandomly(points);
		System.out.println("Query point: " + queryPoint.getX() + ", " + queryPoint.getY() + ".");
		//Get distances map
		Map<Double, Pivot> distanceMap = PivotIndex.getDistanceMap(points, pivots, queryPoint);
		//Issue range query
		Scanner pivotScanner =  AccumuloConnectionManager.queryAccumulo("points", "PIVOT", "POJO");
		System.out.println("Performing range query...");
		long rangeQueryBeforeTime = System.currentTimeMillis();
		List<Point> neighbors = ((AccumuloPivotIndex)index).rangeQueryAccumulo(points, pivotScanner, queryPoint, distanceMap, Integer.parseInt(args[11]));
		long rangeQueryAfterTime = System.currentTimeMillis();
		System.out.println("Time taken to perform range query: " + (rangeQueryAfterTime - rangeQueryBeforeTime) + " milliseconds." );
		for(Point neighbor: neighbors){
			System.out.println("Neighbor: " + neighbor.getX() + ", " + neighbor.getY() + 
					".  Distance from query point: " + PivotUtilities.getDistance(queryPoint, neighbor) +
					".");
			if(neighbors.indexOf(neighbor) > 9){
				break;
			}
		}
	}

	public static void main(String [] args){
		if(args.length != 12){
			System.out.println("Usage: bin/accumulo jar lib/Pivots-0.0.1-SNAPSHOT.jar "
					+ "usace.army.mil.erdc.pivots.accumulo.AccumuloPivotTester -t <tablename> -i <instance> -u <user> -p <password "
					+ "--keepers <zookeeper host:port> <filename> <range>");
			System.exit(0);
		} else{
			init(args);
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
