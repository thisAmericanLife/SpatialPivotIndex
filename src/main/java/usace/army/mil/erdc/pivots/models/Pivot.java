package usace.army.mil.erdc.pivots.models;

import java.util.Map;
import java.util.HashMap;

public class Pivot extends Point implements IPoint{

	private Map<IPoint, Double> pivotMap;
	public Pivot(double x, double y) {
		super(x, y);
		setPivotMap(new HashMap<IPoint, Double>());
		// TODO Auto-generated constructor stub
	}
	public Pivot(Point point){
		super(point.getX(), point.getY());
		setPivotMap(new HashMap<IPoint, Double>());
	}
	public Pivot() {
		// TODO Auto-generated constructor stub
	}
	public Map<IPoint, Double> getPivotMap() {
		return pivotMap;
	}
	public void setPivotMap(Map<IPoint, Double> pivotList) {
		this.pivotMap = pivotList;
	}
	
}
