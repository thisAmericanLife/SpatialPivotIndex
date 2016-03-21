package usace.army.mil.erdc.pivots.models;

import java.util.Map;
import java.util.HashMap;

public class Pivot extends Point implements IPoint{
	private String pivotID;
	private Map<Point, Double> pivotMap;
	public Pivot(double x, double y) {
		super(x, y);
		setPivotMap(new HashMap<Point, Double>());
		// TODO Auto-generated constructor stub
	}
	public Pivot(Point point){
		super(point.getX(), point.getY());
		setPivotMap(new HashMap<Point, Double>());
	}
	public Pivot(Point point, String UID){
		super(point.getX(), point.getY());
		this.pivotID = UID;
		setPivotMap(new HashMap<Point, Double>());
	}
	public Pivot() {
		// TODO Auto-generated constructor stub
	}
	public Map<Point, Double> getPivotMap() {
		return pivotMap;
	}
	public void setPivotMap(Map<Point, Double> pivotList) {
		this.pivotMap = pivotList;
	}
	public String getPivotID() {
		return pivotID;
	}
	public void setPivotID(String pivotID) {
		this.pivotID = pivotID;
	}
	
}
