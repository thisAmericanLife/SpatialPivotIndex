package usace.army.mil.erdc.pivots.models;

import java.io.Serializable;

public interface IPoint extends Serializable{
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

