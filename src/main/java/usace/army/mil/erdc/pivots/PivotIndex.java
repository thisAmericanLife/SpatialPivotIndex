package usace.army.mil.erdc.pivots;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.math.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.GeometryFactory;

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
	
	//From the formula k = d * Pi * r^2, we can solve for r to derive the value of 
	//	initial range query in the series of successive calls to solve kNN.
	//	Otherwise stated, this become r = sqrt(k / d * Pi)
	private static double deriveRange(int k, double density){
		System.out.println("density: " + density);
		System.out.println("Math.PI: " + Math.PI);
		System.out.println("density * Math.PI: " + (density * Math.PI));
		System.out.println("k / density * Math.PI: " + (k / (density * Math.PI)));
		return Math.sqrt(k / (density * Math.PI));
	}
	
	//TODO: Implement L1 Median, hacking for now
	//Returns the median distance of a given set of points
	//http://stackoverflow.com/questions/11955728/how-to-calculate-the-median-of-an-array
	private static double getDensity(Pivot pivot){
		// Get a DescriptiveStatistics instance
		DescriptiveStatistics stats = new DescriptiveStatistics();
		double [] distances = new double[pivot.getPivotMap().values().size()];
		int i = 0;
		for(double distance : pivot.getPivotMap().values()){
			distances[i] = distance;
			stats.addValue(distance);
			i++;
		}
		Median m = new Median();
		Arrays.sort(distances);
		double med = m.evaluate(distances, 50.0);
		System.out.println("Max: " + distances[0] + ", min: " + distances[distances.length - 1]);
		System.out.println("Median: " + med);
		System.out.println("True median: " + stats.getPercentile(50));
		return stats.getGeometricMean();
		
	}
	
	public List<Point> kNNQuery(List<Point> points, List<Pivot> pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, int k){
		List<Point> kNN = new ArrayList<Point>();
		// Find which pivot is closest to the query point (get each and lookup distance).
		Pivot pivot = getClosestPivot(points, pivots, queryPoint);
		//Calculate density of the pivot (L1 Median)
		double density = getDensity(pivot);
		//Get value of first range query
		double range = deriveRange(k, density);
		//Issue successive range query until kNN found
		while(kNN.size() < k){
			kNN.addAll(rangeQuery(points, pivots, queryPoint, 
					distanceMap, range));
			System.out.println("Range: " + range + ", number of neighbors: " + kNN.size());
			range = deriveRange(k++, density);
			
		}
		System.out.println("number of neighbors: " + kNN.size());
		return kNN;
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
		List<Point> neighbors = new ArrayList<Point>();
		for(CandidatePoint candidate : candidatePoints){
			if(! candidate.equals(queryPoint)){	
				candidate.setDistanceToQueryPoint(PivotUtilities.getDistance(candidate, queryPoint));
				sortedCandidates.put(candidate, 1);
			}
		}
		for(Map.Entry<CandidatePoint, Integer> kvPair : sortedCandidates.entrySet()){
			neighbors.add((Point) kvPair.getKey());
		}
		return neighbors;
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
			Map<Point, Double> pivotMap = pivot.getPivotMap();
			for(Point point: points){
				if(! pivotMap.containsKey(point)){
				pivot.getPivotMap().put(point, PivotUtilities.getDistance(pivot, point));
				} else{
					System.out.println("Duplicate key in pivot distance map.");
				}
			}
		}
		return pivots;
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
	
	private static void writePointsToWRT(List<Point> points){
			try{
			File file = new File("/tmp/WKT.csv");
	
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
	
	private List<Point> getConvexHullPoints(List<Point> points){
		//Convert to Array, as this works better with the convex hull computation
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(points), new GeometryFactory());
		return PivotUtilities.convertCoordArrayToPointList(convexHull.getConvexHull().getCoordinates());
	}

	public List<Pivot> choosePivotsSparseSpatialIndex(List<Point> points, boolean useConvexHull){
		List<Pivot> pivots = new ArrayList<Pivot>();
		//Get maximum distance-- brute force for now :/
		double maximumDistance = 0.0;
		double alpha = 0.37;
		//Loop through each point to point maximum distance between objects in collection
		if(useConvexHull){
			List<Point> boundaryPoints = getConvexHullPoints(points);
			maximumDistance = PivotUtilities.searchForMaxDistanceInParallel(boundaryPoints);
		} else{
			maximumDistance = PivotUtilities.searchForMaxDistanceInParallel(points);
		}
		System.out.println("Maximum distance between any two points: " + maximumDistance);
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
