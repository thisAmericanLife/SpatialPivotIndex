package usace.army.mil.erdc.pivots.storm;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PivotRefineBolt extends BaseBasicBolt{
	private static final long serialVersionUID = -979091566418701095L;
	
	private static double range;
	private static Point queryPoint;
	private static int recordCounter = 0;
	private static long startTime = 0;
	
	public PivotRefineBolt(Point queryPoint, double range){
		PivotRefineBolt.range = range;
		PivotRefineBolt.queryPoint = queryPoint;
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
//		if(startTime == 0){
//			startTime = System.currentTimeMillis();
//		}
		Point candidate = (Point) input.getValueByField("point");
		double actualDistance = PivotUtilities.getDistance(candidate, queryPoint);
		if(actualDistance <= range){
			if(startTime == 0){
				startTime = System.currentTimeMillis();
			}
			recordCounter++;
			System.out.println("Winner");
			//System.out.println("Records: " + recordCounter);
			if(recordCounter  == 8){
				System.out.println("Time taken: " + (System.currentTimeMillis() -startTime));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
