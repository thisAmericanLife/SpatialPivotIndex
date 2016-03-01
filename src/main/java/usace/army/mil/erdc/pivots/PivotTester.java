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
import usace.army.mil.erdc.pivots.models.IPivotIndex;
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
	
	private static List<Point> populatePointsFromCaliforniaRoadsDataset(){
		List<Point> points = new ArrayList<Point>();
		try (Stream<String> stream = Files.lines(Paths.get(CALIFORNIA_ROADS_PATH),Charset.defaultCharset())) {
			stream
			.forEach(e -> points.add(new Point(Double.parseDouble(Arrays.asList(e.split(" ")).get(1)),
					Double.parseDouble(Arrays.asList(e.split(" ")).get(2)))));
		} catch (IOException ex) {
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
	
	private static void runTest(List<Point> points, IPivotIndex pivotIndex){
		
		//Select 100 points at random from the newly create points to be pivots
		//List<Pivot> pivots = choosePivotsRandomly(points);
		long startTime = System.currentTimeMillis();
		
		
		//Select pivots via Sparse Spatial Indexing
		System.out.println("Selecting pivots.");
		List<Pivot> pivots = pivotIndex.choosePivotsSparseSpatialIndex(points);
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
		/*for(Map.Entry<Double, Pivot> kvPair : distanceMap.entrySet()){
					System.out.println("Pivot(" + kvPair.getValue().getX() + ", " + kvPair.getValue().getY() +
							") is " + kvPair.getKey() + " units from query pngoint.");
				}*/
		//Solve range query for specified range and number of neighbors
		System.out.println("Performing range query...");
		double range = 250.0;
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

	private static void runPivotTest(){
		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IPivotIndex pivotIndex = indexFactory.getIndex(IPivotIndex.PivotIndexType.SINGLE_NODE);
		//Randomly populate points from 0 - 1000
		//List<Point> points = PivotIndex.populatePointsRandomly();
		//System.out.println("Starting pivot test on random points....");
		//runTest(points, pivotIndex);
		//Create points from file, California Road Network's Nodes
		//http://www.cs.fsu.edu/~lifeifei/SpatialDataset.htm
		
		//List<Point> californiaPoints = populatePointsFromCaliforniaRoadsDataset();

		//System.out.println("Running test on points from California Roads...");
		//runTest(californiaPoints, pivotIndex);
		long startTime = System.currentTimeMillis();
		
		List<Point> walkingDeadPoints = populatePointsFromWalkingDeadDataset();
		long endTime = System.currentTimeMillis();
		System.out.println("Time taken to populate dataset: " + (endTime - startTime) + " milliseconds.");
		System.out.println("Dataset includes " + walkingDeadPoints.size() + " distinct observations");
		runTest(walkingDeadPoints, pivotIndex);

	}

	public static void main(String [] args){
		runPivotTest();
	}
}
