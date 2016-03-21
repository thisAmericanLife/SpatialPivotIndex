package usace.army.mil.erdc.pivots.accumulo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.models.CandidatePoint;
import usace.army.mil.erdc.pivots.models.IIndexingScheme;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import usace.army.mil.erdc.pivots.models.PointFactory;

public class AccumuloPivotIndex extends PivotIndex implements IIndexingScheme {
	final private static Gson gson = new Gson();
	private BatchWriterOpts bwOpts;
	private Scanner pointScanner;
	private int numPivots = 0;

	public AccumuloPivotIndex(){
		super();
	}
	public AccumuloPivotIndex(BatchWriterOpts bwOpts){
		super();
		this.bwOpts = bwOpts;
	}

	public static Map<Double, Pivot> getDistanceMap(Scanner pivots, Point queryPoint){
		Map<Double, Pivot> sortedDistances = new TreeMap<Double, Pivot>();
		double closest = Double.MAX_VALUE;
		String pivotKey = "";
		Map<Point, Double> pivotDistanceMap = null;
		
		Scanner mapScanner = AccumuloConnectionManager.queryAccumulo("points", "PIVOT", "MAP");
		for(Entry<Key,Value> mapEntry : mapScanner) {
			//Use this to determine UID of whichever pivot is the closest
			//Get each pivot, get distance map, get the distance between the pivot and query point
			//		make a Map<Double,Pivot> of these values, then return
			
			
			Map<Point, Double> distanceMap = gson.fromJson(mapEntry.getValue().toString(), 
					new TypeToken<Map<Double, Pivot>>() { }.getType());
			if(distanceMap.get(queryPoint) < closest){
				closest = distanceMap.get(queryPoint);
				pivotKey = mapEntry.getKey().toString(); //Use this to query accumulo for string
			}
			
		}
		Scanner pivotScanner = AccumuloConnectionManager.queryAccumulo(pivotKey, "points", "PIVOT", "MAP");
		for(Entry<Key,Value> mapEntry : pivotScanner) {
			Pivot pivot = gson.fromJson(mapEntry.getValue().toString(), Pivot.class);
			pivot.setPivotMap(pivotDistanceMap);
		}
		
		
				
		for(Entry<Key,Value> pivotEntry : pivots) {
			
			
			
			File file = new File("/tmp/jsonBreaker.txt");

			/* This logic will make sure that the file 
			 * gets created if it is not present at the
			 * specified location*/
			try {
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter fw = new FileWriter(file);
				BufferedWriter bw = new BufferedWriter(fw);

				bw.write(pivotEntry.getValue().toString());
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}





			//System.out.println("Pivot Entry: " + pivotEntry.getValue().toString());
			System.out.println("Pivot Key: " + pivotEntry.getKey().toString());
			Pivot pivot = new JSONDeserializer<Pivot>().deserialize( pivotEntry.getValue().toString() );
			System.out.println("Pivot x: " + pivot.getX());
			System.out.println("Pivot y: " + pivot.getY());
			/*Pivot pivot = 
					gson.fromJson(pivotEntry.getValue().toString(), Pivot.class);*/
			sortedDistances.put(pivot.getPivotMap().get(queryPoint), pivot);
		}
		System.out.println("Success!");
		return sortedDistances;
	}

	private Pivot getClosestPivotAccumulo(Scanner points, Scanner pivots, Point candidatePoint){ 
		/*IPoint closestPivot = pointFactory.getPoint(IPoint.PointType.PIVOT);
		Map<Double, Pivot> distanceMap = getDistanceMap(pivots, candidatePoint);
		if(distanceMap.values().iterator().hasNext()){
			closestPivot = distanceMap.values().iterator().next();
		}
		return (Pivot)closestPivot; */
		Map<Double, Pivot> sortedDistances = new TreeMap<Double, Pivot>();
		double closest = Double.MAX_VALUE;
		String pivotKey = "";
		Map<Point, Double> pivotDistanceMap = null;
		
		Scanner mapScanner = AccumuloConnectionManager.queryAccumulo("points", "PIVOT", "MAP");
		for(Entry<Key,Value> mapEntry : mapScanner) {
			//Use this to determine UID of whichever pivot is the closest
			//Get each pivot, get distance map, get the distance between the pivot and query point
			//		make a Map<Double,Pivot> of these values, then return
			
			
			Map<Point, Double> distanceMap = gson.fromJson(mapEntry.getValue().toString(), 
					new TypeToken<Map<Double, Pivot>>() { }.getType());
			if(distanceMap.get(candidatePoint) < closest){
				closest = distanceMap.get(candidatePoint);
				pivotKey = mapEntry.getKey().toString(); //Use this to query accumulo for string
			}
			
		}
		Scanner pivotScanner = AccumuloConnectionManager.queryAccumulo(pivotKey, "points", "PIVOT", "MAP");
		Pivot pivot = null;
		for(Entry<Key,Value> mapEntry : pivotScanner) {
			pivot = gson.fromJson(mapEntry.getValue().toString(), Pivot.class);
			pivot.setPivotMap(pivotDistanceMap);
			break;
		}
		return pivot;
	}

	private Pivot getClosestPivotAccumulo(Scanner pivots, Point queryPoint){ 
		//Loop through each pivot and perform distance calculation
		//It seems intuitive to get the pivot map for each pivot, since this is precomputed
		//	However, this is significantly higher storage complexity.  This stategy is 
		//	designed with scalability in mind. (i.e.: it would likely be faster to perform 
		//	distance computations on each pivot (since the pivot list is small) than to 
		//	retrieve and iterate through each map list)
		Pivot closestPivot = null;
		double shortestDistance = Double.MAX_VALUE;
		for(Entry<Key,Value> mapEntry : pivots){
			Pivot pivot = gson.fromJson(mapEntry.getValue().toString(), Pivot.class);
			double temporaryDistance = PivotUtilities.getDistance(pivot, queryPoint);
			if(temporaryDistance < shortestDistance){
				closestPivot = pivot;
				shortestDistance = temporaryDistance;
			}
		}
		return closestPivot;
	}

	//TODO: make into first MR-job
	private List<CandidatePoint> runPivotHeuristicAccumulo(Scanner points, Scanner pivots, Point queryPoint, 
			Map<Double, Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = new ArrayList<CandidatePoint>();
		for(Entry<Key,Value> entry : points) {
			try{
				Point currentPoint = (Point)pointFactory.getPoint(IPoint.PointType.POINT);
				JSONObject jsonObj = new JSONObject(entry.getValue().toString());
				currentPoint.setX(Double.parseDouble(jsonObj.getString("x")));
				currentPoint.setY(Double.parseDouble(jsonObj.getString("y")));

				Pivot closestPivot = getClosestPivotAccumulo(points, currentPoint);
				Map<Point, Double> pivotMap = closestPivot.getPivotMap();
				double queryPointToPivotDist = pivotMap.get(queryPoint);
				if(pivotMap.get(currentPoint) == null){
					System.out.println("Null...");
				}
				double currentPointToPivotDist = pivotMap.get(currentPoint) == null ? Double.MAX_VALUE : pivotMap.get(currentPoint);
				//Check for triangle inequality (d(x,z) ≤ d(x,y) + d(y,z))
				if(range >= queryPointToPivotDist + currentPointToPivotDist){
					candidatePoints.add((CandidatePoint)pointFactory.getPoint(IPoint.PointType.CANDIDATE, currentPoint));
				}
			} catch(JSONException e){
				e.printStackTrace();
			}


		}
		return candidatePoints;
	}
	
	//Helper method to retrieve map entry in constant time
	//Map Entry is in format:
	//	RowID : <PivotUID>_<EntryUID>
	//	Column Family : MAP
	//	Column Qualifier : <PointUID>
	private double getPrecomputedDistanceFromAccumulo(Point point, Pivot pivot){
		double distance = 0.0;
		new StringBuilder().append(pivot.getPivotID()).append(point.getUID()).toString();
		Scanner scanner = AccumuloConnectionManager.queryAccumulo("points", new StringBuilder().append(pivot.getPivotID()).append(point.getUID()).toString(),
				"MAP", "ENTRY");
		for(Entry<Key,Value> scannerEntry : scanner) {
			PivotMapEntry mapEntry = gson.fromJson(scannerEntry.getValue().toString(), PivotMapEntry.class);
			//Verify that we have the map entry for the correct point
			if(mapEntry.getPointID().equals(point.getUID())){
				distance = mapEntry.getDistance();
			}
			break;
		}
		return distance;
	}

	private Scanner runPivotHeuristicAccumulo(Scanner points, Scanner pivots,
			Point queryPoint,  double range, BatchWriterConfig bwConfig){
		List<CandidatePoint> candidatePoints = new ArrayList<CandidatePoint>();
		List<Mutation> mutations = new ArrayList<Mutation>();
		int batchWriterIndex = 0;
		System.out.println("Begin heuristic...");
		for(Entry<Key,Value> pointEntry : points) {
			Point currentPoint = gson.fromJson(pointEntry.getValue().toString(), Point.class);
			Pivot closestPivot = getClosestPivotAccumulo(points,currentPoint);
			
			//Get the pre-computed distance between the current point and 
			//	the pivot from Accumulo
			double queryPointToPivotDist = getPrecomputedDistanceFromAccumulo(queryPoint, closestPivot);
			
			double currentPointToPivotDist = getPrecomputedDistanceFromAccumulo(currentPoint, closestPivot);
			//Check for triangle inequality (d(x,z) ≤ d(x,y) + d(y,z))
			if(range >= queryPointToPivotDist + currentPointToPivotDist){
				//Write back to accumulo
				//Later, get each point, convert to CandidatePoint if within range, add to sorted list
				mutations.add(AccumuloConnectionManager.getMutation(currentPoint.getUID(), "POINT",
						"ISCANDIDATE", "TRUE"));
				batchWriterIndex++;
				//note -- maybe don't flush.  or write the mutation in the above if every time, or just do it once
			}
			if(batchWriterIndex > 499){
				AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
				mutations.clear();
				batchWriterIndex = 0;
			}
		}
		return AccumuloConnectionManager.queryAccumulo("points", "POINT", "ISCANDIDATE");
	}

	public Scanner rangeQueryAccumulo(Scanner points, Scanner pivots, Point queryPoint, double range, BatchWriterConfig bwConfig){
		Scanner candidatePoints = runPivotHeuristicAccumulo(points, pivots, queryPoint, range, bwConfig);
		Map<CandidatePoint, Integer> sortedCandidates = new TreeMap<CandidatePoint, Integer>();
		List<Mutation> mutations = new ArrayList<Mutation>();
		List<Point> neighbors = new ArrayList<Point>();
		int batchWriterIndex = 0;
		
		//Iterate through candidate scanner
		//	For each entry, perform lookup (constant time) of candidate, convert to point, then write results back to accumulo
		//	After completion of query, perform clean-up (i.e.: delete ISCANDIDATE rows and results)
		for(Entry<Key,Value> candidateEntry : candidatePoints) {
			System.out.println("In the candidate loop...");
			//Get point
			Point currentPoint = null;
			System.out.println("FYI, we're scanning with this UID: " + candidateEntry.getKey().getRow().toString());
			Scanner pointScanner = AccumuloConnectionManager.queryAccumulo("points", candidateEntry.getKey().getRow().toString(),
					"POINT", "POJO");
			for(Entry<Key,Value> pointEntry : pointScanner) {
				System.out.println("In the point scan loop...");
				currentPoint = gson.fromJson(pointEntry.getValue().toString(), Point.class);
				break;
			}
			if(! currentPoint.equals(queryPoint)){	
				//Perform distance query
				double actualDistance = PivotUtilities.getDistance(currentPoint, queryPoint);
				if(actualDistance <= range){
					//Write to accumulo
					mutations.add(AccumuloConnectionManager.getMutation(currentPoint.getUID(), "POINT",
							"WITHINRANGE", gson.toJson(currentPoint, Point.class)));
					batchWriterIndex++;
				}
				if(batchWriterIndex > 499){
					AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
					mutations.clear();
					batchWriterIndex = 0;
				}
			}
		}
		return AccumuloConnectionManager.queryAccumulo("points", "POINT", "WITHINRANGE");
	}
	
	protected void writePivotsToAccumulo(List<Pivot> pivots, BatchWriterConfig bwConfig){
		List<Mutation> mutations  = new ArrayList<Mutation>();
		System.out.println("Pivot size: " + pivots.size());
		numPivots = pivots.size();
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		for(Pivot pivot : pivots) {
			/*mutations.add(AccumuloConnectionManager.getMutation("pivot_" + index, 
					"PIVOT", "POJO", gson.toJson(pivot, Pivot.class)));*/
			
			//mutations.add(AccumuloConnectionManager.getMutation(pivot.getUID(),
				//	"PIVOT", "MAP", gson.toJson(pivot.getPivotMap())));
			System.out.println("Pivot UID: " + pivot.getPivotID());
			System.out.println("Pivot X: " + pivot.getX());
			System.out.println("Pivot Y: " + pivot.getY());
			pivot.setPivotMap(null);
			
			mutations.add(AccumuloConnectionManager.getMutation(pivot.getPivotID(), 
					"PIVOT", "POJO",  gson.toJson(pivot, Pivot.class)));
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
	}

//	protected void writePivotsToAccumulo(List<Pivot> pivots, Scanner points, BatchWriterConfig bwConfig){
//		List<Mutation> mutations  = new ArrayList<Mutation>();
//		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
//		for(Pivot pivot : pivots) {
//			/*mutations.add(AccumuloConnectionManager.getMutation("pivot_" + index, 
//					"PIVOT", "POJO", gson.toJson(pivot, Pivot.class)));*/
//			
//			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot.getUID(),
//					"PIVOT", "MAP", gson.toJson(pivot.getPivotMap())));
//			if(pivot.getPivotMap() != null) {
//				pivot.setPivotMap(null);
//			}
//			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot.getUID(), 
//					"PIVOT", "POJO",  gson.toJson(pivot, Pivot.class)));
//			
//		}
//		AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
//	}
	
	protected int getDatasetSize(){
		int datasetSize = 0;
		for(Entry<Key,Value> entrySet : 
			AccumuloConnectionManager.queryAccumulo("points", "!!!POINT_COUNT", "DATASET", "COUNT")) {
			datasetSize = Integer.parseInt(entrySet.getValue().toString());
		}
		return datasetSize;
	}
	
	private List<Point> getConvexHullPoints(Scanner points){
		//Convert to Array, as this works better with the convex hull computation
		System.out.println("Dataset size: " + getDatasetSize());
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(points, getDatasetSize()), new GeometryFactory());
		return PivotUtilities.convertCoordArrayToPointList(convexHull.getConvexHull().getCoordinates());
	}
	
	public void populatePivotMapValues(Scanner pivots, Scanner points, BatchWriterConfig bwConfig){
		List<Mutation> mutations = new ArrayList<Mutation>();
		int entryCounter = 0;
		int batchWriterIndex = 0;
		for(Entry<Key,Value> pivotEntrySet : pivots){
			Pivot pivot = gson.fromJson(pivotEntrySet.getValue().toString(), Pivot.class);
			for(Entry<Key,Value> pointEntrySet : points){
				Point point = gson.fromJson(pointEntrySet.getValue().toString(), Point.class);
				PivotMapEntry entry = new PivotMapEntry("entry_" + entryCounter, pivot.getPivotID(), point.getUID(), 
						PivotUtilities.getDistance(pivot, point));
				mutations.add(AccumuloConnectionManager.getMutation(pivot.getPivotID() + "_" + point.getUID(), "MAP",
						"ENTRY", gson.toJson(entry, PivotMapEntry.class)));
				entryCounter++;
				batchWriterIndex++;
				//Flush every 500 values
				if(batchWriterIndex > 499){
					AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
					mutations.clear();
					batchWriterIndex = 0;
				}
			}
		}
	}
	
	public void choosePivotsSparseSpatialIndex(Scanner points, BatchWriterConfig bwConfig, boolean useConvexHull){
		List<Pivot> pivots = new ArrayList<Pivot>();
		Point randomPoint = null;
		//Get maximum distance-- brute force for now :/
		double maximumDistance = 0.0;
		double alpha = 0.37;
		//Loop through each point to point maximum distance between objects in collection
		if(useConvexHull){
			List<Point> boundaryPoints = getConvexHullPoints(points);
			//Get random point
			for(Entry<Key,Value> entrySet : points){
				randomPoint = gson.fromJson(entrySet.getValue().toString(), Point.class);
				break;
			}
			maximumDistance = PivotUtilities.searchForMaxDistanceInParallel(boundaryPoints);
		} else{
			PointFactory pointFactory = new PointFactory();
			int i = 0;
			double temporaryDistance = 0.0;
			for(Entry<Key,Value> outerEntrySet : points){
				Point outterPoint = gson.fromJson(outerEntrySet.getValue().toString(), Point.class);
				for(Entry<Key,Value> innerEntrySet : points){
					Point innerPoint = gson.fromJson(outerEntrySet.getValue().toString(), Point.class);
					if(! innerPoint.equals(outterPoint)){
						temporaryDistance = PivotUtilities.getDistance(outterPoint, innerPoint);
						if(temporaryDistance > maximumDistance){
							maximumDistance = temporaryDistance;
							//Since we've already gone into this if, check if this should be our "randomly selected" point
							if(i == 500){
								randomPoint = innerPoint;
							}
						}
					}
				}
				i++;
			}
		}
		System.out.println("Maximum distance between any two points: " + maximumDistance);
		//Loop through each point...again...to determine if each point candidate satisfies 
		//	M = max { d ( x, y ) /x,y ∈ U }
		//	M(α), where α is definied as a double between 0.35 and 0.4
		//	Oscar Pedreira and Nieves R. Brisaboa, "Spatial Selection of Sparse Pivots for Similarity
		//	Search in Metric Spaces"

		//Add first point in the list
		Pivot firstPivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, randomPoint);
		firstPivot.setPivotID("pivot_0");
		int id = 1;
		pivots.add(firstPivot);
		for(Entry<Key,Value> entrySet : points){
			Point point = gson.fromJson(entrySet.getValue().toString(), Point.class);
			boolean satisfiesPivotCriteria = true;
			for(int j = 0; j < pivots.size(); j++){
				if(! (PivotUtilities.getDistance(point, pivots.get(j)) >= (maximumDistance * alpha))){
					//TODO: add interface
					satisfiesPivotCriteria = false;
				}
			}
			if(satisfiesPivotCriteria){
				Pivot pivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, point);
				pivot.setPivotID(new StringBuilder().append("pivot_").append(String.valueOf(id)).toString());
				
				pivots.add(pivot); 
				id++;
			}
		}
		writePivotsToAccumulo(pivots, bwConfig);
	}

	public void populatePointsRandomlyAccumulo(double max, double min){
		List<Mutation> mutations = new ArrayList<Mutation>();
		//Populate random points
		for(int i = 0; i < 10000; i ++){
			IPoint point = pointFactory.getPoint(IPoint.PointType.POINT);
			point.setX(ThreadLocalRandom.current().nextDouble(min, max));
			point.setY(ThreadLocalRandom.current().nextDouble(min, max));

			mutations.add(AccumuloConnectionManager.getMutation("point_" + i, "POINT", "POJO", gson.toJson(point, Point.class)));
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts);
	}

	private class PivotMapEntry{
		private String UID;
		private String pointID;
		private String pivotID;
		private double distance;
		public PivotMapEntry(String UID, String pivotID, String pointID, double distance){
			this.UID = UID;
			this.pivotID = pivotID;
			this.pointID = pointID;
			this.distance = distance;
		}
		public String getPointID() {
			return pointID;
		}
		public void setPointID(String pointID) {
			this.pointID = pointID;
		}
		public double getDistance() {
			return distance;
		}
		public void setDistance(double distance) {
			this.distance = distance;
		}
		public String getUID() {
			return UID;
		}
		public void setUID(String UID) {
			this.UID = UID;
		}

	}
}
