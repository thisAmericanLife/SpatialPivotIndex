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

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Point candidate = (Point) input.getValueByField("point");
		double actualDistance = PivotUtilities.getDistance(candidate, PivotKnnTopology.getQueryPoint());
		if(actualDistance <= (PivotKnnTopology.getRange()  * candidate.getNumStormIterations())){
			PivotKnnTopology.incrementNumNeighborsFound();
			if(PivotKnnTopology.getNumNeighborsFound() == PivotKnnTopology.getNumNeighborsFound()){
				System.out.println("Done.");
			}
		} else {
			//Increase number of iterations
			candidate.incrememntNumStormIterations();
			//Send back into cycle
			collector.emit(new Values(candidate));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("point"));
	}

}
