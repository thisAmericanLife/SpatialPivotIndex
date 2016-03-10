import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import usace.army.mil.erdc.Pivots.Utilities.ParallelSearchThread;
import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;


public class ParallelSearchThreadTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";
	private final static String WALKING_DEAD_TWEETS_PATH = "/home/ktyler/Documents/misc/twitter_sm.tsv";
	
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
	
	public static void main(String [] args){
		//List<Point> points = populatePointsFromCaliforniaRoadsDataset();
		List<Point> points = populatePointsFromWalkingDeadDataset();
		Double quarterPointFloat = new Double(points.size() / 4.0);
		Double midpointFloat = new Double(points.size() / 2.0);
		int quarterPoint = quarterPointFloat.intValue();
		int midpoint = midpointFloat.intValue();
		System.out.println("Dataset size: " + points.size());
		System.out.println("Quarter point: " + quarterPoint);
		System.out.println("Mid point: " + midpoint);
		ParallelSearchThread thread1 = new ParallelSearchThread(points, points.subList(0, quarterPoint));
		ParallelSearchThread thread2 = new ParallelSearchThread(points, points.subList(quarterPoint + 1, midpoint));
		ParallelSearchThread thread3 = new ParallelSearchThread(points, points.subList(midpoint + 1, midpoint + quarterPoint));
		ParallelSearchThread thread4 = new ParallelSearchThread(points, points.subList(midpoint + quarterPoint + 1, points.size()));
		//ParallelSearchThread thread2 = new ParallelSearchThread(points, points.subList(midpoint + 1, points.size() - 1));
		
		
		try{
			long startTime = System.currentTimeMillis();
			thread1.start();
			thread2.start();
			thread3.start();
			thread4.start();
			
			thread1.join();
			thread2.join();
			thread3.join();
			thread4.join();
			long endTime = System.currentTimeMillis();
			System.out.println("Time taken to complete multi-threaded search: " + (endTime - startTime)  + " milliseconds." );
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		System.out.println("Maxmimum distance within dataset: " + ParallelSearchThread.getMaxDistance());
		
		/*long startTimeN2 = System.currentTimeMillis();
		double temporaryDistance = 0;
		double maximumDistance = 0;
		for(Point outterPoint: points){
			for(Point innerPoint: points){
				if(! outterPoint.equals(innerPoint)){
					temporaryDistance = PivotUtilities.getDistance(outterPoint, innerPoint);
					if(temporaryDistance > maximumDistance){
						maximumDistance = temporaryDistance;
					}
				} 
			}
		}
		long n2EndTime = System.currentTimeMillis();
		System.out.println("Time taken to complete single-threaded search: " + (n2EndTime - startTimeN2)  + " milliseconds." );
		System.out.println("Actual maximum distance: " + maximumDistance);*/
	}
}
