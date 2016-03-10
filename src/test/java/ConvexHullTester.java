import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;


public class ConvexHullTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";
	private final static String WALKING_DEAD_TWEETS_PATH = "/home/ktyler/Documents/misc/twitter_sm.tsv";
	private static void writePointsToWRT(List<Point> points, String filename){
		try{
		File file = new File(filename);

		// if file doesn't exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		for(Point point : points){
			bw.write("\"Point (" + point.getX() + " " + point.getY() + ")\"\n");
		}
		
		bw.close();
		} catch(IOException e){
			e.printStackTrace();
		}

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
	
	private static List<Point> getUniquePoints(IPoint [] points){
		Set<IPoint> uniquePoints = new HashSet<IPoint>();
		List<IPoint> duplicatePoints = new ArrayList<IPoint>();
		List<Point> pointList = new ArrayList<Point>();
		for(int i = 0; i < points.length; i++){
			if(uniquePoints.contains(points[i])){
				duplicatePoints.add(points[i]);
			} else{
					if(i != points.length -1){
						uniquePoints.add(points[i]);
						pointList.add((Point)points[i]);
					}
			}
		}
		return pointList;
	}
	
	private static void runOReillyConvexHullTest(List<Point> californiaPoints, List<Point> zombiePoints){
		//Test California Roads Dataset
				
				Point [] caliPoints = new Point[californiaPoints.size()];
				caliPoints = californiaPoints.toArray(caliPoints);
				writePointsToWRT(californiaPoints, "/tmp/californiaPoints.csv");
				
				
				IPoint [] convexHull = PivotUtilities.computeConvexHullOReilly(caliPoints);
				List<Point> uniqueHullPoints = getUniquePoints(convexHull);
				writePointsToWRT(uniqueHullPoints, "/tmp/californiaHullPoints.csv");
				
				assert(convexHull.length < caliPoints.length);
				System.out.println("Original dataset size: " + caliPoints.length);
				System.out.println("Convex hull size: " + convexHull.length);
				System.out.println("Unique hull size: " + uniqueHullPoints.size());
				
				//Test Walking Dead Twitter Dataset
				
				writePointsToWRT(zombiePoints, "/tmp/zombiePoints.csv");
				Point [] zombiePointsArray = new Point[californiaPoints.size()];
				zombiePointsArray = zombiePoints.toArray(zombiePointsArray);
				
				IPoint [] zombieHull = PivotUtilities.computeConvexHullOReilly(zombiePointsArray);
				List<Point> uniqueZombieHullPoints = getUniquePoints(zombieHull);
				
				writePointsToWRT(uniqueZombieHullPoints, "/tmp/zombieHullPoints.csv");
				
				assert(zombieHull.length < zombiePointsArray.length);
				if(zombieHull.length < zombiePointsArray.length){
					System.out.println("Successful convex hull computation achieved");
				} else{
					System.out.println("Convex hull formed erroneously.");
				}
				System.out.println("Original dataset size: " + zombiePointsArray.length);
				System.out.println("Convex hull size: " + zombieHull.length);
				System.out.println("Unique hull size: " + uniqueZombieHullPoints.size());
				//1,132,878
				//1,238,920
	}
	
	
	
	private static void runJTSConvexHullTest(List<Point> californiaPoints, List<Point> zombiePoints){
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(californiaPoints), new GeometryFactory());
		writePointsToWRT(PivotUtilities.convertCoordArrayToPointList(convexHull.getConvexHull().getCoordinates()), "/tmp/californiaHullPoints_JTS.csv");
		
		ConvexHull zombieConvexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(zombiePoints), new GeometryFactory());
		writePointsToWRT(PivotUtilities.convertCoordArrayToPointList(zombieConvexHull.getConvexHull().getCoordinates()), "/tmp/zombieHullPoints_JTS.csv");
	}
	
	public static void main(String [] args){
		List<Point> californiaPoints = populatePointsFromCaliforniaRoadsDataset();
		List<Point> zombiePoints = populatePointsFromWalkingDeadDataset();
		//runOReillyConvexHullTest(californiaPoints, zombiePoints);
		runJTSConvexHullTest(californiaPoints, zombiePoints);
	}
}
