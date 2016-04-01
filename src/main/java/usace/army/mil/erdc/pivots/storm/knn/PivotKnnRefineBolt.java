package usace.army.mil.erdc.pivots.storm.knn;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PivotKnnRefineBolt extends BaseBasicBolt{
	private static final long serialVersionUID = -3277141588587798884L;
	private static double range = 0.0;
	private static int neighborsFound = 0;
	private static int neighborsToFind = 0;
	private static Point queryPoint;
	
	public PivotKnnRefineBolt(Point queryPoint, int neighborsToFind, double range){
		PivotKnnRefineBolt.queryPoint = queryPoint;
		PivotKnnRefineBolt.neighborsToFind = neighborsToFind;
		PivotKnnRefineBolt.range = range;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Point candidate = (Point) input.getValueByField("point");
	//	System.out.println("Got a point in refine: " + PivotKnnRefineBolt.counter);
		double actualDistance = PivotUtilities.getDistance(candidate, queryPoint);
		if(actualDistance <= (range  * candidate.getNumStormIterations())){
			neighborsFound++;
			if(neighborsFound == neighborsToFind){
				long endTime = System.currentTimeMillis();
				PivotKnnTopology.killTopology(endTime - PivotKnnFilterBolt.startTime, endTime);
			}
		} else {
			//Increase number of iterations
			candidate.incrememntNumStormIterations();
			//Send back into cycle
		//	System.out.println("Emmitting... "+ candidate.getUID());
			collector.emit(new Values(candidate));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("point"));
	}

}
