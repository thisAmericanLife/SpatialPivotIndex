package usace.army.mil.erdc.pivots.storm;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.accumulo.AccumuloConnectionManager;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PivotFilterBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;    
	private static List<Pivot> pivots;
	private static Map<String,Double> pivotMap;
	private static double range = 0.0;
	private static Connector connector;
	public static long startTime;
	private static boolean processingStarted = false;
	public PivotFilterBolt(List<Pivot> pivots, Map<String,Double> pivotMap, Connector connector, double range){
		PivotFilterBolt.connector = connector;
		PivotFilterBolt.pivots = pivots;
		PivotFilterBolt.pivotMap = pivotMap;
		PivotFilterBolt.range = range;
	}
	
	private Pivot getClosestPivotAccumulo(Point currentPoint){ 
		//Loop through each pivot and perform distance calculation
		//It seems intuitive to get the pivot map for each pivot, since this is precomputed
		//	However, this is significantly higher storage complexity.  This stategy is 
		//	designed with scalability in mind. (i.e.: it would likely be faster to perform 
		//	distance computations on each pivot (since the pivot list is small) than to 
		//	retrieve and iterate through each map list)
		Pivot closestPivot = null;
		double shortestDistance = Double.MAX_VALUE;
		for(Pivot pivot: pivots){
			double temporaryDistance = PivotUtilities.getDistance(pivot, currentPoint);
			if(temporaryDistance < shortestDistance){
				closestPivot = pivot;
				shortestDistance = temporaryDistance;
			}
		}
		return closestPivot;
	}

	private double getPrecomputedDistanceFromAccumulo(Point point, Pivot pivot){
		double distance = 0.0;
		Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter("pointsIndex", 
				pivot.getPivotID(),
				point.getUID(), "PIVOT", connector);
		for(Entry<Key,Value> scannerEntry : scanner) {
				distance = Double.parseDouble(scannerEntry.getKey().getColumnQualifier().toString());
				break;
		}
		return distance;
	}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(! PivotFilterBolt.processingStarted){
			PivotFilterBolt.startTime = System.currentTimeMillis();
			PivotFilterBolt.processingStarted = true;
		}
		Point point = (Point)input.getValueByField("point");
		Pivot pivot = getClosestPivotAccumulo(point);
		double queryPointToPivotDist = pivotMap.get(pivot.getPivotID());
		
		double currentPointToPivotDist = getPrecomputedDistanceFromAccumulo(point, pivot);
		
		if(range <= queryPointToPivotDist + currentPointToPivotDist){
			collector.emit(new Values(point));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("point"));
	}

}
