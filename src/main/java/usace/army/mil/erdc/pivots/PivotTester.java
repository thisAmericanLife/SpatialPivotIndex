package usace.army.mil.erdc.pivots;

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
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;

public class PivotTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";

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
	
	private static void runTest(List<Point> points, PivotIndex pivotIndex){
		
		//Select 100 points at random from the newly create points to be pivots
		//List<Pivot> pivots = choosePivotsRandomly(points);

		//Select pivots via Sparse Spatial Indexing
		List<Pivot> pivots = pivotIndex.choosePivotsSparseSpatialIndex(points);
		
		//Populate pivot information
		pivots = pivotIndex.populatePivotMapValues(pivots, points);

		//Issue range Query for randomly selected point
		//Get randomly selected point from newly created points
		Point queryPoint = selectPointFromListRandomly(points);
		System.out.println("Query point: " + queryPoint.getX() + ", " + queryPoint.getY() + ".");
		//Get distances map
		Map<Double, Pivot> distanceMap = PivotIndex.getDistanceMap(points, pivots, queryPoint);
		/*for(Map.Entry<Double, Pivot> kvPair : distanceMap.entrySet()){
					System.out.println("Pivot(" + kvPair.getValue().getX() + ", " + kvPair.getValue().getY() +
							") is " + kvPair.getKey() + " units from query point.");
				}*/
		//Solve range query for specified range and number of neighbors
		double range = 2.5;
		List<Point> nearestNeighbors = pivotIndex.rangeQuery(points, pivots, queryPoint, 
				distanceMap, range);
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
		PivotIndex pivotIndex = new PivotIndex();
		//Randomly populate points from 0 - 1000
		//List<Point> points = PivotIndex.populatePointsRandomly();
		//System.out.println("Starting pivot test on random points....");
		//runTest(points, pivotIndex);
		//Create points from file, California Road Network's Nodes
		//http://www.cs.fsu.edu/~lifeifei/SpatialDataset.htm
		List<Point> californiaPoints = populatePointsFromCaliforniaRoadsDataset();

		System.out.println("Running test on points from California Roads...");
		runTest(californiaPoints, pivotIndex);


	}

	public static void main(String [] args){
		runPivotTest();
	}
}
