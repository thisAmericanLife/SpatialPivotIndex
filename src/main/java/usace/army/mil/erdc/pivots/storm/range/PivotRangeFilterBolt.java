package usace.army.mil.erdc.pivots.storm.range;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PivotRangeFilterBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 4637115083884270396L;
	final private static Gson gson = new Gson();
	private static List<Pivot> pivots;
	private static Map<String,Double> pivotMap;
	private static double range = 0.0;
	static long startTime = 0;
	public PivotRangeFilterBolt(List<Pivot> pivots, Map<String,Double> pivotMap, double range){
		PivotRangeFilterBolt.pivots = pivots;
		PivotRangeFilterBolt.pivotMap = pivotMap;
		PivotRangeFilterBolt.range = range;
	}

	private Pivot getClosestPivot(Point currentPoint){ 
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
	
	private double getPrecomputedDistance(Point point, Pivot pivot){
		return point.getDistancesToPivot().get(pivot.getPivotID());
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if(startTime== 0){
			PivotRangeFilterBolt.startTime = System.currentTimeMillis();
		}
		Point point;
		if(! input.contains("str")){
			point = (Point) input.getValueByField("point");
		} else{
			point = gson.fromJson(input.getValueByField("str").toString(), Point.class);
		}
		//System.out.println("Got a point in filter: " + PivotKnnFilterBolt.counter);
		Pivot pivot = getClosestPivot(point);
		double queryPointToPivotDist = pivotMap.get(pivot.getPivotID());

		double currentPointToPivotDist = getPrecomputedDistance(point, pivot);
		if((range)>= queryPointToPivotDist + currentPointToPivotDist){
			collector.emit(new Values(point));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("point"));
	}

}
