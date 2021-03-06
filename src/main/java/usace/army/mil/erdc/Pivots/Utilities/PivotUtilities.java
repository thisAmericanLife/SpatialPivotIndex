package usace.army.mil.erdc.Pivots.Utilities;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Coordinate;

import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;
import usace.army.mil.erdc.pivots.models.oReilly.PartialHull;
import usace.army.mil.erdc.pivots.models.oReilly.QuickSortExternal;

public class PivotUtilities {
	
	private static final int NUM_THREADS = 4;
	private static final int ITERATIONS = 1000;
	private static final int KEY_LENGTH = 192; // bits
	//Static utility functions
		static final public double getDistance(IPoint source, IPoint target){
			return Math.sqrt(Math.pow((target.getX() - source.getX()),2) + Math.pow((target.getY() - source.getY()),2));
		}
		
		
		public static IPoint[] computeConvexHullOReilly (final IPoint[] points) {
		    // sort by x-coordinate (and if ==, by y-coordinate). 
		    final int n = points.length;
		    
		    // sort with available threads, using R=4 sweet spot.
		    QuickSortExternal<IPoint> qs = 
		      new QuickSortExternal<IPoint>(points, Point.xy_sorter);
		    qs.setPivotMethod(qs.lastSelector());
		    qs.setNumberHelperThreads(NUM_THREADS);
		    qs.setThresholdRatio(4);
		    
		    // trivial cases can return now.
		    if (n < 3) { return points; }
		  
		    // from this point on, we only use two threads.
		    final PartialHull upper =new PartialHull(points[0],points[1]);
		    final PartialHull lower =new PartialHull(points[n-1],points[n-2]);
		    
		    Thread up = new Thread() {
		      public void run() {
		        // Compute upper hull by starting with leftmost two points
		        for (int i = 2; i < n; i++) {
		          upper.add(points[i]);
		          while (upper.hasThree() && upper.areLastThreeNonRight()) {
		            upper.removeMiddleOfLastThree();
		          }
		        }
		      }
		    };
		    
		    Thread down = new Thread() {
		      public void run() {
		        // Compute lower hull by starting with rightmost two points
		        for (int i = n-3; i >=0; i--) {
		          lower.add(points[i]);
		          while (lower.hasThree() && lower.areLastThreeNonRight()) {
		            lower.removeMiddleOfLastThree();
		          }
		        }
		      }
		    };
		    
		    // start both threads and wait until both are done.
		    up.start();
		    down.start();
		    try {
		      up.join();
		      down.join();
		    } catch (InterruptedException ie) {
		      System.err.println("Multithreaded execution interrupted.");
		    }
		    
		    // remove duplicate end points when combining. Transcribe the 
		    // partial hulls into the array return value.
		    IPoint[] hull = new IPoint[upper.size()+lower.size()-2];
		    int num = upper.transcribe (hull, 0);
		    lower.transcribe (hull, num-1, lower.size() - 2);
		    
		    return hull;
		}
		
		private static List<AccumuloParallelSearchThread> getParallelSearchThreads(Scanner points, int size){
			List<AccumuloParallelSearchThread> threads = new ArrayList<AccumuloParallelSearchThread>();
			Double quarterPointFloat = new Double(size / 4.0);
			Double midpointFloat = new Double(size / 2.0);
			int quarterPoint = quarterPointFloat.intValue();
			int midpoint = midpointFloat.intValue();
			
			/*threads.add(new AccumuloParallelSearchThread(points, points.subList(0, quarterPoint)));
			threads.add(new AccumuloParallelSearchThread(points, points.subList(quarterPoint + 1, midpoint)));
			threads.add(new AccumuloParallelSearchThread(points, points.subList(midpoint + 1, midpoint + quarterPoint)));
			threads.add(new AccumuloParallelSearchThread(points, points.subList(midpoint + quarterPoint + 1, points.size())));*/
			
			return null;
		}
		
		private static List<ParallelSearchThread> getParallelSearchThreads(List<Point> points){
			List<ParallelSearchThread> threads = new ArrayList<ParallelSearchThread>();
			Double quarterPointFloat = new Double(points.size() / 4.0);
			Double midpointFloat = new Double(points.size() / 2.0);
			int quarterPoint = quarterPointFloat.intValue();
			int midpoint = midpointFloat.intValue();
			
			threads.add(new ParallelSearchThread(points, points.subList(0, quarterPoint)));
			threads.add(new ParallelSearchThread(points, points.subList(quarterPoint + 1, midpoint)));
			threads.add(new ParallelSearchThread(points, points.subList(midpoint + 1, midpoint + quarterPoint)));
			threads.add(new ParallelSearchThread(points, points.subList(midpoint + quarterPoint + 1, points.size())));
			
			return threads;
		}
		
		public static double searchForMaxDistanceInParallel(List<Point> points){
			List<ParallelSearchThread> threads = getParallelSearchThreads(points);
			try{
				//Start threads
				for(ParallelSearchThread thread : threads){
					thread.start();
				}
				//Join threads and wait until completion of tasks
				for(ParallelSearchThread thread : threads){
					thread.join();
				}
			}catch(InterruptedException e){
				System.out.println("Execution of multithreaded search interrupted.");
			}
			return ParallelSearchThread.getMaxDistance();
		}
		
		public static Coordinate [] convertPointListToCoordArray(List<Point> points){
			Coordinate [] coordinates = new Coordinate[points.size()];
			int i = 0;
			for(Point point : points){
				coordinates[i] = new Coordinate(point.getX(), point.getY());
				i++;
			}
			return coordinates;
		}
		
		public static String hashUID(String UID){
		    char[] uidChars = UID.toCharArray();
		    byte[] saltBytes = "salt".getBytes();
		    byte[] hashedUID = null;

		    PBEKeySpec spec = new PBEKeySpec(
		    		uidChars,
		        saltBytes,
		        ITERATIONS,
		        KEY_LENGTH
		    );
		    SecretKeyFactory key;
			try {
				key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
				hashedUID = key.generateSecret(spec).getEncoded();
			} catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    //pre-process
			String uid = String.format("%x", new BigInteger(hashedUID));
			/*if(uid.charAt(0) == '-'){
				StringBuilder sb = new StringBuilder();
				sb.append(uid.substring(1));
				return sb.toString();
			} else{
				return uid;
			}*/
			return uid;
		  }
		
		//For Accumulo
		public static Coordinate [] convertPointListToCoordArray(Scanner points, int datasetSize){
			Gson gson = new Gson();
			Coordinate [] coordinates = new Coordinate[datasetSize- 1];
			int i = 0;
			for(Entry<Key,Value> entrySet : points){
				Point point = gson.fromJson(entrySet.getValue().toString(), Point.class);
				coordinates[i] = new Coordinate(point.getX(), point.getY());
				i++;
			}
			System.out.println("i from point list to array: " + i);
			System.out.println("i from point list to array: " + coordinates.length);
			return coordinates;
		}
		
		public static List<Point> convertCoordArrayToPointList(Coordinate [] coordinates){
			List<Point> pointList = new ArrayList<Point>();
			for(int i = 0; i < coordinates.length; i++){
				pointList.add(new Point(coordinates[i].x, coordinates[i].y));
			}
			System.out.println("pointlist size: " + pointList.size());
			return pointList;
		}
		
		public static double calculateGeometricMedian(Pivot pivot){
			return 0.0;
		}
}
