package usace.army.mil.erdc.pivots;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Map;
import java.util.stream.Stream;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.IIndexingScheme;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.PivotIndexFactory;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class PivotTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";
	private final static String WALKING_DEAD_TWEETS_PATH = "/home/ktyler/Documents/misc/twitter_sm.tsv";
	private static Point selectPointFromListRandomly(List<Point> points){
		Random random = new Random();
		return points.get(random.nextInt(10000));
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
	
	 private static List<Point> populatePointsFromWalkingDeadDataset(){
			PointFactory pointFactory = new PointFactory();
			List<Point> points = new ArrayList<Point>();
			try (BufferedReader br = new BufferedReader(new FileReader(WALKING_DEAD_TWEETS_PATH))) {
				String line;
				while ((line = br.readLine()) != null) {
					String [] delimitedString = line.split("\t");
					IPoint point = pointFactory.getPoint(IPoint.PointType.POINT);
					point.setX(Double.parseDouble(delimitedString[6]));
					point.setY(Double.parseDouble(delimitedString[5]));
					points.add((Point)point);
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
			return points;
		}
	
	private static void runTest(List<Point> points, IIndexingScheme pivotIndex, boolean rangeQuery, boolean kNNQuery, double range, int k){
		
		//Select 100 points at random from the newly create points to be pivots
		//List<Pivot> pivots = choosePivotsRandomly(points);
		long startTime = System.currentTimeMillis();
		
		
		//Select pivots via Sparse Spatial Indexing
		System.out.println("Selecting pivots.");
		List<Pivot> pivots = (List<Pivot>) pivotIndex.choosePivotsSparseSpatialIndex(points, true);
		long pivotSelectionTime = System.currentTimeMillis();
		System.out.println("Time taken to select pivots: " + (pivotSelectionTime - startTime)  + " milliseconds." );
		//Populate pivot information
		System.out.println("Mapping pivots...");
		pivots = pivotIndex.populatePivotMapValues(pivots, points);
		long pivotMapTime = System.currentTimeMillis();
		System.out.println("Time taken to map pivots: " + (pivotMapTime - pivotSelectionTime)  + " milliseconds." );

		//Issue range Query for randomly selected point
		//Get randomly selected point from newly created points
		Point queryPoint = selectPointFromListRandomly(points);
		System.out.println("Query point: " + queryPoint.getX() + ", " + queryPoint.getY() + ".");
		//Get distances map
		Map<Double, Pivot> distanceMap = PivotIndex.getDistanceMap(points, pivots, queryPoint);
		
		//Perform range query for specified range and number of neighbors
		if(rangeQuery){
			System.out.println("Performing range query...");
			List<Point> nearestNeighbors = pivotIndex.rangeQuery(points, pivots, queryPoint, 
					distanceMap, range);
			long rangeQueryTime = System.currentTimeMillis();
			System.out.println("Time taken to perform range query: " + (rangeQueryTime - pivotMapTime) + " milliseconds." );
			System.out.println("Nearest neighbors: ");
			for(Point neighbor: nearestNeighbors){
				System.out.println("Neighbor: " + neighbor.getX() + ", " + neighbor.getY() + 
						".  Distance from query point: " + PivotUtilities.getDistance(queryPoint, neighbor) +
						".");
				if(nearestNeighbors.indexOf(neighbor) > 9){
					break;
				}
			}
		} 
		//Perform kNN query
		else if(kNNQuery){
			System.out.println("Performing kNN query...");
			List<Point> kNN = pivotIndex.kNNQuery(points, pivots, queryPoint, distanceMap, k);
			long kNNQueryTime = System.currentTimeMillis();
			System.out.println("Time taken to perform kNN query: " + (kNNQueryTime - pivotMapTime) + " milliseconds." );
		}
	}

	private static void runPivotTest(){
		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IIndexingScheme pivotIndex = indexFactory.getIndex(IIndexingScheme.PivotIndexType.SINGLE_NODE);
		//Randomly populate points from 0 - 1000
		//List<Point> points = PivotIndex.populatePointsRandomly();
		//System.out.println("Starting pivot test on random points....");
		//runTest(points, pivotIndex);
		//Create points from file, California Road Network's Nodes
		//http://www.cs.fsu.edu/~lifeifei/SpatialDataset.htm
		long startTime = System.currentTimeMillis();
		List<Point> californiaPoints = populatePointsFromWalkingDeadDataset();
		long endTime = System.currentTimeMillis();
		System.out.println("Running test on points from California Roads...");
		System.out.println("Dataset includes " + californiaPoints.size() + " distinct observations");
		//runTest(californiaPoints, pivotIndex, false, true, 250.0, 15);
		
		//For range query -- 
		//runTest(californiaPoints, pivotIndex, false, true, 250.0, 15);
		runTest(californiaPoints, pivotIndex, false, true, 250.0, 275000);
		
		
		/*long startTime = System.currentTimeMillis();
		List<Point> walkingDeadPoints = populatePointsFromWalkingDeadDataset();
		long endTime = System.currentTimeMillis();
		System.out.println("Time taken to populate dataset: " + (endTime - startTime) + " milliseconds.");
		System.out.println("Dataset includes " + walkingDeadPoints.size() + " distinct observations");
		runTest(walkingDeadPoints, pivotIndex, true, false, 250.0, 15);*/

	}

	public static void main(String [] args){
		runPivotTest();
	}
}
