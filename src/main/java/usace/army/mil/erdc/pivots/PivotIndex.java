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
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.Pivots.models.Quadrant;
import usace.army.mil.erdc.pivots.models.CandidatePoint;
import usace.army.mil.erdc.pivots.models.IIndexingScheme;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class PivotIndex implements IIndexingScheme {
	//TODO: Grid pivots
	//		Add check for if query point is pivot
	//		Add OMNI check for intrinsic dimensionality
	//		Add convex hull to avoid n^2 index creation
	
	protected static PointFactory pointFactory;
	private double minX;
	private double minY;
	private double maxX;
	private double maxY;
	private com.vividsolutions.jts.geom.Point centroid;
	private Quadrant upperLeft, upperRight, lowerLeft, lowerRight;
	
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
	
	//Returns the density of the quadrant for which the point lies in
	private static double getDensity(Point point){
		return getQuadrant(point).getDensity();
	}
	
	public List<Point> kNNQuery(List<Point> points, List<Pivot> pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, int k){
		List<Point> kNN = new ArrayList<Point>();
		// Find which pivot is closest to the query point (get each and lookup distance).
		Pivot pivot = getClosestPivot(points, pivots, queryPoint);
		//Calculate density of the quadrant for which the point falls in
		double density = getDensity(queryPoint);
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
				if(candidate.getDistanceToQueryPoint() <= range){
					sortedCandidates.put(candidate, 1);
				}
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
	
	private void instantiateQuadrants(){
		Coordinate [] upperLeftCoordinateArray, upperRightCoordinateArray,
		lowerRightCoordinateArray, lowerLeftCoordinateArray= new Coordinate[4];
		
		upperLeftCoordinateArray[0] = new Coordinate(centroid.getX(), centroid.getY());
		upperLeftCoordinateArray[1] = new Coordinate(minX, centroid.getY());
		upperLeftCoordinateArray[2] = new Coordinate(minX, maxY);
		upperLeftCoordinateArray[3] = new Coordinate(centroid.getX(), maxY);
		upperLeft = new Quadrant(getQuadrantArea(upperLeftCoordinateArray));
		
		upperRightCoordinateArray[0] = new Coordinate(centroid.getX(), centroid.getY());
		upperRightCoordinateArray[1] = new Coordinate(centroid.getX(), maxX);
		upperRightCoordinateArray[2] = new Coordinate(maxX, maxY);
		upperRightCoordinateArray[3] = new Coordinate(maxX, centroid.getY());
		upperRight = new Quadrant(getQuadrantArea(upperRightCoordinateArray));
		
		lowerRightCoordinateArray[0] = new Coordinate(centroid.getX(), centroid.getY());
		lowerRightCoordinateArray[1] = new Coordinate(maxX, centroid.getY());
		lowerRightCoordinateArray[2] = new Coordinate(maxX, minY);
		lowerRightCoordinateArray[3] = new Coordinate(centroid.getX(), minY);
		lowerRight = new Quadrant(getQuadrantArea(lowerRightCoordinateArray));
		
		lowerLeftCoordinateArray[0] = new Coordinate(centroid.getX(), centroid.getY());
		lowerLeftCoordinateArray[1] = new Coordinate(centroid.getY(), minY);
		lowerLeftCoordinateArray[2] = new Coordinate(minX, minY);
		lowerLeftCoordinateArray[3] = new Coordinate(minX, centroid.getY());
		lowerLeft = new Quadrant(getQuadrantArea(lowerLeftCoordinateArray));
	}
	
	private Quadrant getQuadrant(Point point){
		double x = point.getX();
		double y = point.getY();
		
		if(x <= centroid.getX() && y >= centroid.getY()){
			return upperLeft;
		} else if(x >= centroid.getX() && y >= centroid.getY()){
			return upperRight;
		} else if(x >= centroid.getX() && y <= centroid.getY()){
			return lowerRight;
		} else{
			return lowerLeft;
		}
	}
	
	private double getQuadrantArea(Coordinate [] coordinates){
		GeometryFactory geomFactory = new GeometryFactory();
		LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
		Polygon polygon = new Polygon(linear, null, geomFactory);
		return polygon.getArea();
	}
	
	//(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny). 
	private void setEnvelopeValues(List<Point> points, 
			com.vividsolutions.jts.geom.Point centroidPoint){
		minX = points.get(0).getX();
		minY = points.get(0).getY();
		maxX = points.get(1).getX();
		maxY = points.get(2).getY();
		centroid = new Point(centroidPoint.getX(), centroidPoint.getY());
		//Create quadrants from minimum bounding box of convex hull
		instantiateQuadrants();
	}
	
	private List<Point> getConvexHullPoints(List<Point> points){
		//Convert to Array, as this works better with the convex hull computation
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(points), new GeometryFactory());
		//(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny). 
		setEnvelopeValues(PivotUtilities.convertCoordArrayToPointList(convexHull.getConvexHull()
				.getEnvelope().getCoordinates(), convexHull.getConvexHull().getCentroid()));
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
		Pivot firstPivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, points.get(0));
		firstPivot.setUID("0");
		pivots.add(new Pivot(points.get(0)));
		for(int i = 1; i < points.size(); i++){
			Point point = points.get(i);
			//Perform point-in-polygon query to determine which quadrant the point lies in
			//Update counter for density
			getQuadrant(point).incrememntObservations();
			
			boolean satisfiesPivotCriteria = true;
			for(int j = 0; j < pivots.size(); j++){
				if(! (PivotUtilities.getDistance(point, pivots.get(j)) >= (maximumDistance * alpha))){
					//TODO: add interface
					satisfiesPivotCriteria = false;
				}
			}
			if(satisfiesPivotCriteria){
				Pivot pivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, point);
				pivot.setUID(String.valueOf(i));
				pivots.add(pivot); 
			}
		}
		//Calculate density for each quadrant
		upperLeft.setDensity(upperLeft.getDensity());
		upperRight.setDensity(upperRight.getDensity());
		lowerLeft.setDensity(lowerLeft.getDensity());
		lowerRight.setDensity(lowerRight.getDensity());
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
