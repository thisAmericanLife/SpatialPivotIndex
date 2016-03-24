package usace.army.mil.erdc.Pivots.Utilities;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.gson.Gson;

import usace.army.mil.erdc.pivots.accumulo.AccumuloConnectionManager;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;

public class AccumuloParallelSearchThread extends Thread{
	final private static Gson gson = new Gson();
	
	/*public AccumuloParallelSearchThread(Scanner points, Scanner pivots){
		this.points = points;
	}
	
	@Override
	public void run() {
		double temporaryDistance = 0;
		for(Point outterPoint: points){
			for(Point innerPoint: pointsSubset){
				if(! outterPoint.equals(innerPoint)){
					temporaryDistance = PivotUtilities.getDistance(outterPoint, innerPoint);
					if(temporaryDistance > maxDistance){
						maxDistance = temporaryDistance;
					}
				}
			}
		}
		
	}*/
	
	/*public void runTest(Scanner points){
		for(Entry<Key,Value> pointEntry : points) {
			long inForLoopTime = System.currentTimeMillis();
			Point currentPoint = gson.fromJson(pointEntry.getValue().toString(), Point.class);
			Pivot closestPivot = getClosestPivotAccumulo(pivots,currentPoint);
			long closestPivotTime = System.currentTimeMillis();
		//	System.out.println("Closest pivot selection: " + (closestPivotTime - inForLoopTime));
			//System.out.println("Time taken to get closest pivot: " + (timeTakenToGetClosetPivot - startHueristicTime));
			//Get the pre-computed distance between the current point and 
			//	the pivot from Accumulo
			long beginPrecomputedDistTime  = System.currentTimeMillis();
			double queryPointToPivotDist = pivotDistancesToQueryPoint.get(closestPivot.getPivotID());

			double currentPointToPivotDist = getPrecomputedDistanceFromAccumulo(currentPoint, closestPivot);
			long endPrecomputedDistTime  = System.currentTimeMillis();
			//System.out.println("Closest pivot selection: " + (endPrecomputedDistTime - beginPrecomputedDistTime));
			//System.out.println("Time taken to get precomputed distances: " + (timeTakenForLookup - timeTakenToGetClosetPivot));
			//Check for triangle inequality (d(x,z) ≤ d(x,y) + d(y,z))
			if(range >= queryPointToPivotDist + currentPointToPivotDist){
				//Write back to accumulo
				//Later, get each point, convert to CandidatePoint if within range, add to sorted list
				mutations.add(AccumuloConnectionManager.getMutation(currentPoint.getUID(), "POINT",
						"ISCANDIDATE", "TRUE"));
				batchWriterIndex++;
				//note -- maybe don't flush.  or write the mutation in the above if every time, or just do it once
			}
			if(batchWriterIndex > 50000){
				System.out.println("Writing...");
				AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
				mutations.clear();
				batchWriterIndex = 0;
			}
			long endForLoopTime = System.currentTimeMillis();
			totalTime += (endForLoopTime - inForLoopTime);
			if((endForLoopTime - inForLoopTime) > 10){
				System.out.println("Loop time: " + (endForLoopTime - inForLoopTime) 
						+ ", closest pivot searcH: " + (closestPivotTime - inForLoopTime) 
						+ ", precomputed dist search: " + (endPrecomputedDistTime - beginPrecomputedDistTime));
				
			}
			
		}
	}*/
}
