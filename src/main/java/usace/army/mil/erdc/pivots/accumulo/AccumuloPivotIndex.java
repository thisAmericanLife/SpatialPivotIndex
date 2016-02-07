package usace.army.mil.erdc.pivots.accumulo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import com.google.gson.Gson;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.models.CandidatePoint;
import usace.army.mil.erdc.pivots.models.IPivotIndex;
import usace.army.mil.erdc.pivots.models.IPoint;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;

public class AccumuloPivotIndex extends PivotIndex implements IPivotIndex {
	final private static Gson gson = new Gson();
	private BatchWriterOpts bwOpts;
	private Scanner pointScanner;
	
	public AccumuloPivotIndex(){
		super();
	}
	public AccumuloPivotIndex(BatchWriterOpts bwOpts){
		super();
		this.bwOpts = bwOpts;
	}
	
	public static Map<Double, Pivot> getDistanceMap(Scanner points, Scanner pivots, Point queryPoint){
		Map<Double, Pivot> sortedDistances = new TreeMap<Double, Pivot>();
		for(Entry<Key,Value> pivotEntry : pivots) {
			Pivot pivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, 
					gson.fromJson(pivotEntry.getValue().toString(), Point.class));
			sortedDistances.put(pivot.getPivotMap().get(queryPoint), pivot);
		}
		return sortedDistances;
	}
	
	private Pivot getClosestPivotAccumulo(Scanner points, Scanner pivots, Point candidatePoint){ 
		IPoint closestPivot = pointFactory.getPoint(IPoint.PointType.PIVOT);
		Map<Double, Pivot> distanceMap = getDistanceMap(points, pivots, candidatePoint);
		if(distanceMap.values().iterator().hasNext()){
			closestPivot = distanceMap.values().iterator().next();
		}
		return (Pivot)closestPivot;
	}

	//TODO: make into first MR-job
	private List<CandidatePoint> runPivotHeuristicAccumulo(Scanner points, Scanner pivots, Point queryPoint, 
			Map<Double, Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = new ArrayList<CandidatePoint>();
		for(Entry<Key,Value> entry : pointScanner) {
			Point currentPoint = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, 
					gson.fromJson(entry.getValue().toString(), Point.class));
			Pivot closestPivot = getClosestPivotAccumulo(points, pivots, currentPoint);
			double queryPointToPivotDist = closestPivot.getPivotMap().get(queryPoint);
			double currentPointToPivotDist = closestPivot.getPivotMap().get(currentPoint);
			//Check for triangle inequality (d(x,z) ≤ d(x,y) + d(y,z))
			if(range >= queryPointToPivotDist + currentPointToPivotDist){
				candidatePoints.add((CandidatePoint)pointFactory.getPoint(IPoint.PointType.CANDIDATE, currentPoint));
			}
		}
		return candidatePoints;
	}
	
	public List<Point> rangeQueryAccumulo(Scanner points, Scanner pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = runPivotHeuristicAccumulo(points, pivots, queryPoint, distanceMap, range);
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
	
	private void writePivotsToAccumulo(Scanner pivots, Scanner points){
		List<Mutation> mutations  = new ArrayList<Mutation>();
		int index = 0;
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		for(Entry<Key,Value> pivotEntry : pivots) {
			Pivot pivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, 
					gson.fromJson(pivotEntry.getValue().toString(), Point.class));
			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
					"PIVOT", "POJO", gson.toJson(pivot, Point.class)));
			for(Entry<Key,Value> pointEntry : points) {
				Point point = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, 
						gson.fromJson(pointEntry.getValue().toString(), Point.class));
				PivotMapEntry pivotMapEntry = new PivotMapEntry(point.getUID(),
						PivotUtilities.getDistance(pivot, point));
				mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
						"PIVOT", "DISTANCE_MAP_ENTRY", gson.toJson(pivotMapEntry, PivotMapEntry.class)));
			}
			index++;
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts);
	}
	
	private void writePivotsToAccumulo(List<Pivot> pivots, Scanner points){
		List<Mutation> mutations  = new ArrayList<Mutation>();
		int index = 0;
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		for(Pivot pivot : pivots) {
			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
					"PIVOT", "POJO", gson.toJson(pivot, Point.class)));
			for(Entry<Key,Value> pointEntry : points) {
				Point point = (Pivot)pointFactory.getPoint(IPoint.PointType.POINT, 
						gson.fromJson(pointEntry.getValue().toString(), Point.class));
				PivotMapEntry pivotMapEntry = new PivotMapEntry(point.getUID(),
						PivotUtilities.getDistance(pivot, point));
				mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
						"PIVOT", "DISTANCE_MAP_ENTRY", gson.toJson(pivotMapEntry, PivotMapEntry.class)));
			}
			index++;
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts);
	}
	
	public void choosePivotsRandomlyAccumulo(Scanner points, int numPivots){
		List<Pivot> pivots = new ArrayList<Pivot>();
		List<Point> pointList = new ArrayList<Point>();
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		 for(Entry<Key,Value> entry : points) {
			 Point point = (Pivot)pointFactory.getPoint(IPoint.PointType.POINT, 
						gson.fromJson(entry.getValue().toString(), Point.class));
			 pointList.add(point);
         }
		Random random = new Random();
		for(int i = 0; i < numPivots; i++){
			pivots.add((Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, pointList.get(random.nextInt(10000))));
		}
		writePivotsToAccumulo(pivots, points);
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
		private String pivotID;
		private double distance;
		public PivotMapEntry(String pivotID, double distance){
			this.pivotID = pivotID;
			this.distance = distance;
		}
		public String getPivotID() {
			return pivotID;
		}
		public void setPivotID(String pivotID) {
			this.pivotID = pivotID;
		}
		public double getDistance() {
			return distance;
		}
		public void setDistance(double distance) {
			this.distance = distance;
		}
		
	}
}
