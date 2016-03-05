import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;


public class ConvexHullTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";
	private final static String WALKING_DEAD_TWEETS_PATH = "/home/ktyler/Documents/misc/twitter_sm.tsv";
	private static Point selectPointFromListRandomly(List<Point> points){
		Random random = new Random();
		return points.get(random.nextInt(10000));
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
	
	public static void main(String [] args){
		//Test California Roads Dataset
		List<Point> californiaPoints = populatePointsFromCaliforniaRoadsDataset();
		Point [] caliPoints = new Point[californiaPoints.size()];
		caliPoints = californiaPoints.toArray(caliPoints);
		
		IPoint [] convexHull = PivotUtilities.compute(caliPoints);
		assert(convexHull.length < caliPoints.length);
		System.out.println("Original dataset size: " + caliPoints.length);
		System.out.println("Convex hull size: " + convexHull.length);
		
		//Test Walking Dead Twitter Dataset
		List<Point> zombiePoints = populatePointsFromWalkingDeadDataset();
		Point [] zombiePointsArray = new Point[californiaPoints.size()];
		zombiePointsArray = zombiePoints.toArray(zombiePointsArray);
		
		IPoint [] zombieHull = PivotUtilities.compute(zombiePointsArray);
		assert(zombieHull.length < zombiePointsArray.length);
		if(zombieHull.length < zombiePointsArray.length){
			System.out.println("Successful convex hull computation achieved");
		} else{
			System.out.println("Convex hull formed erroneously.");
		}
		System.out.println("Original dataset size: " + zombiePointsArray.length);
		System.out.println("Convex hull size: " + zombieHull.length);
		//1,132,878
		//1,238,920
	}
}
