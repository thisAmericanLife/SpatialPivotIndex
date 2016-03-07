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
import usace.army.mil.erdc.pivots.models.Point;


public class ParallelSearchThreadTester {
	private final static String CALIFORNIA_ROADS_PATH = "/home/ktyler/Documents/strider/CaliforniaRoadNetworksNodes.txt";
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
		List<Point> points = populatePointsFromCaliforniaRoadsDataset();
		Double quarterPointFloat = new Double(points.size() / 4.0);
		Double midpointFloat = new Double(points.size() / 2.0);
		int quarterPoint = quarterPointFloat.intValue();
		int midpoint = midpointFloat.intValue();
		
		ParallelSearchThread thread1 = new ParallelSearchThread(points.subList(0, quarterPoint));
		ParallelSearchThread thread2 = new ParallelSearchThread(points.subList(quarterPoint + 1, midpoint));
		ParallelSearchThread thread3 = new ParallelSearchThread(points.subList(midpoint + 1, midpoint + quarterPoint));
		ParallelSearchThread thread4 = new ParallelSearchThread(points.subList(midpoint + quarterPoint + 1, points.size()));
		//ParallelSearchThread thread2 = new ParallelSearchThread(points.subList(quarterPoint + 1, points.size() - 1));
		
		thread1.start();
		thread2.start();
		thread3.start();
		thread4.start();
		try{
			thread1.join();
			thread2.join();
			thread3.join();
			thread4.join();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		System.out.println("Maxmimum distance within dataset: " + ParallelSearchThread.getMaxDistance());
		
		
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
		System.out.println("Actual maximum distance: " + maximumDistance);
	}
}
