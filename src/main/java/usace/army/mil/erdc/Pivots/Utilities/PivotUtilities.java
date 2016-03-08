package usace.army.mil.erdc.Pivots.Utilities;

import java.util.ArrayList;
import java.util.List;

import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.oReilly.PartialHull;
import usace.army.mil.erdc.pivots.models.oReilly.QuickSortExternal;

public class PivotUtilities {
	
	private static final int NUM_THREADS = 4;
	//Static utility functions
		static final public double getDistance(IPoint source, IPoint target){
			return Math.sqrt(Math.pow((target.getX() - source.getX()),2) + Math.pow((target.getY() - source.getY()),2));
		}
		
		
		public static IPoint[] compute (final IPoint[] points) {
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
}
