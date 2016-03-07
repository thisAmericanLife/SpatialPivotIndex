package usace.army.mil.erdc.pivots.models;

import java.util.Comparator;

public class Point implements IPoint {

	private double x;
	private double y;
	private String UID;
	/** Numbers within this amount are considered to be the same. */
	public static final double epsilon = 1E-9;
	
	//Constructor
	public Point(double x, double y){
		this.x = x;
		this.y = y;
	}
	
	public Point(){}
	
	//Taken from O/'Reilly FloatingPoint.java
	//https://github.com/heineman/algorithms-nutshell-2ed/blob/master/JavaCode/src/algs/model/FloatingPoint.java
	/**
	 * See if the value is close enough to actually be considered 0.0 and
	 * return 0.0 if need be.
	 * <p>
	 * Otherwise the value is returned.
	 * 
	 * @param x   value being considered
	 */
	public static double value(double x) {
		if ((x >= 0) && (x <= epsilon)) {
			return 0.0;
		}
		
		if ((x < 0) && (-x <= epsilon)) {
			return 0.0;
		}
		
		return x;
	}
	
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
		return (this.x == point.getX() && this.y == point.getY());
	}

	public String getUID() {
		return UID;
	}

	public void setUID(String uID) {
		UID = uID;
	}

	public static Comparator<IPoint> xy_sorter =
			new Comparator<IPoint>() {

				/**
				 * Compares two 2-dimensional points in the Cartesian plane.
				 * 
				 * Handles null values as follows. 
				 * <ol>
				 * <ul>If one is null && two is null, then 0 is returned (an exact null match).
				 * <ul>Else if one is null, then return -1
				 * <ul>Else if two is null, then return +1
				 * </ol> 
				 * 
				 * @param one
				 * @param two
				 */
				public int compare(IPoint one, IPoint two) {
					if (one == null) {
						if (two == null) { 
							return 0; 
						}
						return -1;
					} else if (two == null) { 
						return +1; 
					}
					
					double fp = value(one.getX() - two.getX());
					if (fp < 0) { return -1; }
					if (fp > 0) { return +1; }
					
					fp = value(one.getY() - two.getY());
					if (fp < 0) { return -1; }
					if (fp > 0) { return +1; }
					
					// same exact (x,y) points
					return 0;
				}

		};	
}
