package usace.army.mil.erdc.Pivots.Utilities;

import java.util.List;

import usace.army.mil.erdc.pivots.models.Point;

public class ParallelSearchThread extends Thread{
	private static double maxDistance;
	private List<Point> points;
	private List<Point> pointsSubset;
	
	public ParallelSearchThread(List<Point> points){
		this.points = points;
	}
	public ParallelSearchThread(List<Point> points, List<Point> pointsSubset){
		this.points = points;
		this.pointsSubset = pointsSubset;
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
		
	}
	
	public static double getMaxDistance(){
		return maxDistance;
	}
}
