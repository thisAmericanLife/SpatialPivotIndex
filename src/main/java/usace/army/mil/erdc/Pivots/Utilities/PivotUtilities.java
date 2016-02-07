package usace.army.mil.erdc.Pivots.Utilities;

import usace.army.mil.erdc.pivots.models.Point;

public class PivotUtilities {
	//Static utility functions
		static final public double getDistance(Point source, Point target){
			return Math.sqrt(Math.pow((target.getX() - source.getX()),2) + Math.pow((target.getY() - source.getY()),2));
		}
}
