package usace.army.mil.erdc.pivots.models;

public class CandidatePoint extends Point implements Comparable<CandidatePoint>, IPoint{

	private double distanceToQueryPoint;
	//Constructor
	public CandidatePoint(double x, double y) {
		super(x, y);
		// TODO Auto-generated constructor stub
	}
	
	public CandidatePoint(Point currentPoint) {
		super(currentPoint.getX(), currentPoint.getY());
		// TODO Auto-generated constructor stub
	}

	public CandidatePoint() {
		// TODO Auto-generated constructor stub
	}

	//Getters and setters
	public double getDistanceToQueryPoint() {
		return distanceToQueryPoint;
	}
	public void setDistanceToQueryPoint(double distanceToQueryPoint) {
		this.distanceToQueryPoint = distanceToQueryPoint;
	}

	//Comparator
	@Override
	public int compareTo(CandidatePoint point) {
		return Double.compare(this.distanceToQueryPoint, point.getDistanceToQueryPoint());
	}
	
	@Override
	public boolean equals(Point point){
		return (this.getX() == point.getX() && this.getY() == point.getY());
	}
	
}
