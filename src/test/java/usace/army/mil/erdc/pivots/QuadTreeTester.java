package usace.army.mil.erdc.pivots;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import usace.army.mil.erdc.Pivots.Utilities.PivotUtilities;
import usace.army.mil.erdc.pivots.models.Point;

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.quadtree.Quadtree;

public class QuadTreeTester {
	
	private static void initTest(){
		Quadtree quadTree = new Quadtree();
		List<Point> points = PivotTester.populatePointsFromCaliforniaRoadsDataset();
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(points), new GeometryFactory());
		Envelope envelope = convexHull.getConvexHull().getEnvelopeInternal();
		Coordinate [] coordinates = convexHull.getConvexHull().getCoordinates();
		
		for(Coordinate coord : coordinates){
			quadTree.insert(envelope, coord);
		}
		
	}
	
	public static void main(String [] args){
		initTest();
	}
}
