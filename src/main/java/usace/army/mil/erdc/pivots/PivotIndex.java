package usace.army.mil.erdc.pivots;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.CandidatePoint;
import usace.army.mil.erdc.pivots.models.IPivotIndex;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class PivotIndex implements IPivotIndex {
	//TODO: Grid pivots
	//		Add check for if query point is pivot
	//		Add OMNI check for intrinsic dimensionality
	//		Add convex hull to avoid n^2 index creation
	
	protected static PointFactory pointFactory;
	
	public PivotIndex(){
		pointFactory = new PointFactory();
	}
	
	private static Pivot getClosestPivot(List<Point>points, List<Pivot>pivots, Point candidatePoint){ 
		IPoint closestPivot = pointFactory.getPoint(IPoint.PointType.PIVOT);
		Map<Double, Pivot> distanceMap = getDistanceMap(points, pivots, candidatePoint);
		if(distanceMap.values().iterator().hasNext()){
			closestPivot = distanceMap.values().iterator().next();
		}
		return (Pivot)closestPivot;
	}

	//TODO: make into first MR-job
	private static List<CandidatePoint> runPivotHeuristic(List<Point> points, List<Pivot> pivots, Point queryPoint, 
			Map<Double, Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = new ArrayList<CandidatePoint>();
		for(Point currentPoint : points){
			Pivot closestPivot = getClosestPivot(points, pivots, currentPoint);
			double queryPointToPivotDist = closestPivot.getPivotMap().get(queryPoint);
			double currentPointToPivotDist = closestPivot.getPivotMap().get(currentPoint);
			//Check for triangle inequality (d(x,z) ≤ d(x,y) + d(y,z))
			if(range >= queryPointToPivotDist + currentPointToPivotDist){
				candidatePoints.add((CandidatePoint)pointFactory.getPoint(IPoint.PointType.CANDIDATE, currentPoint));
			}
		}
		return candidatePoints;
	}

	//TODO: make into second MR-job
	public List<Point> rangeQuery(List<Point> points, List<Pivot> pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = runPivotHeuristic(points, pivots, queryPoint, distanceMap, range);
		Map<CandidatePoint, Integer> sortedCandidates = new TreeMap<CandidatePoint, Integer>();
		List<Point> kNN = new ArrayList<Point>();
		for(CandidatePoint candidate : candidatePoints){
			if(! candidate.equals(queryPoint)){	
				candidate.setDistanceToQueryPoint(PivotUtilities.getDistance(candidate, queryPoint));
				sortedCandidates.put(candidate, 1);
			}
		}
		for(Map.Entry<CandidatePoint, Integer> kvPair : sortedCandidates.entrySet()){
			kNN.add((Point) kvPair.getKey());
		}
		return kNN;
	}

	public static Map<Double, Pivot> getDistanceMap(List<Point>points, List<Pivot>pivots, Point queryPoint){
		Map<Double, Pivot> sortedDistances = new TreeMap<Double, Pivot>();
		for(Pivot pivot: pivots){
			sortedDistances.put(pivot.getPivotMap().get(queryPoint), pivot);
		}
		return sortedDistances;
	}

	public List<Pivot> populatePivotMapValues(List<Pivot> pivots, List<Point> points){
		for(Pivot pivot: pivots){
			for(Point point: points){
				pivot.getPivotMap().put(point, PivotUtilities.getDistance(pivot, point));
			}
		}
		return pivots;
	}

	public List<Pivot> choosePivotsSparseSpatialIndex(List<Point> points){
		List<Pivot> pivots = new ArrayList<Pivot>();
		//Get maximum distance-- brute force for now :/
		double maximumDistance = 0.0;
		double temporaryDistance = 0.0;
		double alpha = 0.37;
		//Loop through each point to point maximum distance between objects in collection
		//TODO: consider strategies for avoiding O(n^2) search
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
		//Loop through each point...again...to determine if each point candidate satisfies 
		//	M = max { d ( x, y ) /x,y ∈ U }
		//	M(α), where α is definied as a double between 0.35 and 0.4
		//	Oscar Pedreira and Nieves R. Brisaboa, "Spatial Selection of Sparse Pivots for Similarity
		//	Search in Metric Spaces"

		//Add first point in the list
		//TODO: add interface
		pivots.add(new Pivot(points.get(0)));
		for(int i = 1; i < points.size(); i++){
			Point point = points.get(i);
			boolean satisfiesPivotCriteria = true;
			for(int j = 0; j < pivots.size(); j++){
				if(! (PivotUtilities.getDistance(point, pivots.get(j)) >= (maximumDistance * alpha))){
					//TODO: add interface
					satisfiesPivotCriteria = false;
				}
			}
			if(satisfiesPivotCriteria){
				pivots.add((Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, point)); 
			}
		}
		return pivots;
	}

	public List<Pivot> choosePivotsRandomly(List<Point> points){
		List<Pivot> pivots = new ArrayList<Pivot>();
		int index = 0;
		Random random = new Random();
		for(int i = 0; i < 100; i++){
			index = random.nextInt(10000);
			pivots.add((Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, points.get(index)));
		}
		return pivots;
	}

	public static List<Point> populatePointsRandomly(double max, double min){
		List<Point> points = new ArrayList<Point>();
		//Populate random points
		for(int i = 0; i < 10000; i ++){
			IPoint point = pointFactory.getPoint(IPoint.PointType.POINT);
			point.setX(ThreadLocalRandom.current().nextDouble(min, max));
			point.setY(ThreadLocalRandom.current().nextDouble(min, max));
			points.add((Point) point);
		}
		return points;
	}
}
