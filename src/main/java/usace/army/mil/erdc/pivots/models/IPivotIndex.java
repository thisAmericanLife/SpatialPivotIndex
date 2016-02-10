package usace.army.mil.erdc.pivots.models;

import java.util.List;
import java.util.Map;

public interface IPivotIndex {
	public enum PivotIndexType {
		SINGLE_NODE,
		ACCUMULO
	}
	public List<Pivot> choosePivotsSparseSpatialIndex(List<Point> points);
	
	public List<Pivot> populatePivotMapValues(List<Pivot>pivots, List<Point> points);
	
	public List<Point> rangeQuery(List<Point> points, List<Pivot> pivots, Point queryPoint, Map<Double, 
			Pivot> distanceMap, double range);
}
