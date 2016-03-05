package usace.army.mil.erdc.pivots.models.oReilly;

import usace.army.mil.erdc.pivots.models.IPoint;

/**
 * Originally taken from:http://broadcast.oreilly.com/2009/06/may-column-multithreaded-algor.html#MTJ
 * Adapted by Kevin Tyler to interact with the Java constructs within this project.
 * 
 * Defined interface for algorithms that compute the convex hull for a set
 * of IPoint objects. 
 * 
 * @author George Heineman
 * @version 1.0, 6/15/08
 * @since 1.0
 */
public interface IConvexHull {
	
	/**
	 * Return the computed convex hull for the input set of IPoint objects.
	 * <p>
	 * Points must have at least three points to do anything meaningful. If
	 * it does not, then the sorted array is returned as the "hull".
	 * <p>
	 * Some implementations may be able to work if duplicate points are found,
	 * but the set should contain distinct {@link algs.model.IPoint} objects.
	 *
	 * @param points     an array of (n &ge; 3) two dimensional points.
	 */
	IPoint[] compute (IPoint[] points);
}

