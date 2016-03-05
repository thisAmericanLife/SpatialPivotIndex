package usace.army.mil.erdc.pivots.models;

public interface IPoint{
	public double getX();
	public void setX(double x);
	public double getY();
	public void setY(double y);	
	
	public enum PointType {
		CANDIDATE,
		PIVOT,
		POINT
	}
}

