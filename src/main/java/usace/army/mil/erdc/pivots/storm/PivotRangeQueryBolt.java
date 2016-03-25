package usace.army.mil.erdc.pivots.storm;

import com.google.gson.Gson;

import usace.army.mil.erdc.pivots.models.Point;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PivotRangeQueryBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;       
	final private static Gson gson = new Gson();

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Point point = gson.fromJson(input.getValueByField("str").toString(), Point.class);
		collector.emit(new Values(point));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("point"));

	}

}