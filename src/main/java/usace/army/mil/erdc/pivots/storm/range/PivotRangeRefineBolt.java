package usace.army.mil.erdc.pivots.storm.range;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PivotRangeRefineBolt extends BaseBasicBolt{
	private static final long serialVersionUID = 884205329071629729L;
	private static double range = 0.0;
	private static int neighborsFound = 0;
	private static int neighborsToFind = 0;
	private static Point queryPoint;
	
	public PivotRangeRefineBolt(Point queryPoint, int neighborsToFind, double range){
		PivotRangeRefineBolt.queryPoint = queryPoint;
		PivotRangeRefineBolt.neighborsToFind = neighborsToFind;
		PivotRangeRefineBolt.range = range;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Point candidate = (Point) input.getValueByField("point");
	//	System.out.println("Got a point in refine: " + PivotKnnRefineBolt.counter);
		double actualDistance = PivotUtilities.getDistance(candidate, queryPoint);
		if(actualDistance <= (range)){
			neighborsFound++;
			//System.out.println("num neighbors: " + neighborsFound);
			if(neighborsFound == 2717){
				System.out.println(System.currentTimeMillis() - PivotRangeFilterBolt.startTime);
				System.out.println("Done.");
				//PivotKnnTopology.killTopology(endTime - PivotKnnFilterBolt.startTime, endTime);
			}
		} 
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
