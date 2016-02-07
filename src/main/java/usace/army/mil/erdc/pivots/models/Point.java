package usace.army.mil.erdc.pivots.models;

public class Point implements IPoint {

	private double x;
	private double y;
	private String UID;
	
	//Constructor
	public Point(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	public Point(){}
	
	//Getters and setters
	@Override 
	public double getX() {
		return x;
	}
	@Override 
	public void setX(double x) {
		this.x = x;
	}
	@Override 
	public double getY() {
		return y;
	}
	@Override 
	public void setY(double y) {
		this.y = y;
	}

	public boolean equals(CandidatePoint point) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean equals(Point point) {
		// TODO Auto-generated method stub
		return false;
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String uID) {
		UID = uID;
	}
}
