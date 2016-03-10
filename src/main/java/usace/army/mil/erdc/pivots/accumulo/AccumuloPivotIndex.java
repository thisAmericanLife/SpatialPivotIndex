package usace.army.mil.erdc.pivots.accumulo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;

import flexjson.JSONDeserializer;
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

	public static Map<Double, Pivot> getDistanceMap(Scanner pivots, Point queryPoint){
		Map<Double, Pivot> sortedDistances = new TreeMap<Double, Pivot>();
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

			/*Pivot pivot = 
					gson.fromJson(pivotEntry.getValue().toString(), Pivot.class);*/
			sortedDistances.put(pivot.getPivotMap().get(queryPoint), pivot);
		}
		System.out.println("Success!");
		return sortedDistances;
	}

	private Pivot getClosestPivotAccumulo(Scanner points, Scanner pivots, Point candidatePoint, Map<Double, Pivot> distanceMap){ 
		IPoint closestPivot = pointFactory.getPoint(IPoint.PointType.PIVOT);
		//Map<Double, Pivot> distanceMap = getDistanceMap(pivots, candidatePoint);
		if(distanceMap.values().iterator().hasNext()){
			closestPivot = distanceMap.values().iterator().next();
		}
		return (Pivot)closestPivot;
	}

	private Pivot getClosestPivotAccumulo(List<Point> points, Scanner pivots, Point candidatePoint, Map<Double, Pivot> distanceMap){ 
		IPoint closestPivot = pointFactory.getPoint(IPoint.PointType.PIVOT);
		//Map<Double, Pivot> distanceMap = getDistanceMap(pivots, candidatePoint);
		if(distanceMap.values().iterator().hasNext()){
			closestPivot = distanceMap.values().iterator().next();
		}
		return (Pivot)closestPivot;
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

				Pivot closestPivot = getClosestPivotAccumulo(points, pivots, currentPoint, distanceMap);
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

	private List<CandidatePoint> runPivotHeuristicAccumulo(List<Point> points, Scanner pivots, Point queryPoint, 
			Map<Double, Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = new ArrayList<CandidatePoint>();
		for(Point currentPoint : points) {

			Pivot closestPivot = getClosestPivotAccumulo(points, pivots, currentPoint, distanceMap);
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


		}
		return candidatePoints;
	}

	public List<Point> rangeQueryAccumulo(Scanner points, Scanner pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = runPivotHeuristicAccumulo(points, pivots, queryPoint, distanceMap, range);
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
	
	public List<Point> rangeQueryAccumulo(List<Point> points, Scanner pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, double range){
		List<CandidatePoint> candidatePoints = runPivotHeuristicAccumulo(points, pivots, queryPoint, distanceMap, range);
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

	private void writePivotsToAccumulo(Scanner pivots){
		List<Mutation> mutations  = new ArrayList<Mutation>();
		int index = 0;
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		for(Entry<Key,Value> pivotEntry : pivots) {
			Pivot pivot = (Pivot)pointFactory.getPoint(IPoint.PointType.PIVOT, 
					gson.fromJson(pivotEntry.getValue().toString(), Point.class));
			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
					"PIVOT", "POJO", gson.toJson(pivot, Point.class)));
			for(Entry<Key,Value> pointEntry : pointScanner) {
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

	protected void writePivotsToAccumulo(List<Pivot> pivots, Scanner points, BatchWriterConfig bwConfig){
		List<Mutation> mutations  = new ArrayList<Mutation>();
		int index = 0;
		pointScanner = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO");
		for(Pivot pivot : pivots) {
			mutations.add(AccumuloConnectionManager.getMutation("pivot_" + index, 
					"PIVOT", "POJO", gson.toJson(pivot, Pivot.class)));
			/*for(Entry<Key,Value> pointEntry : points) {
				Point point = (Point)pointFactory.getPoint(IPoint.PointType.POINT, 
						gson.fromJson(pointEntry.getValue().toString(), Point.class));
				PivotMapEntry pivotMapEntry = new PivotMapEntry(point.getUID(),
						PivotUtilities.getDistance(pivot, point));
				mutations.add(AccumuloConnectionManager.getMutation("pivot_" + pivot, 
						"PIVOT", "DISTANCE_MAP_ENTRY", gson.toJson(pivotMapEntry, PivotMapEntry.class)));
			}*/
			index++;
		}
		AccumuloConnectionManager.writeMutations(mutations, bwOpts, bwConfig);
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
