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
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.util.GeometricShapeFactory;

public class QuadTreeTester {
	private static Geometry queryGeom;
	private static void queryTree(Quadtree quadtree, Envelope query){
		GeometryFactory gf = new GeometryFactory();
		List<Coordinate> finalCoords = new ArrayList<Coordinate>();
		List<Coordinate> coords = new ArrayList<Coordinate>();
		long start = System.currentTimeMillis();
		for(Object object: quadtree.query(query)){
			Coordinate coord = (Coordinate) object;
			coords.add(coord);
			//System.out.println("Coordinate: " + coord.x + ", " + coord.y);
		}
		//Filter
		
		for(Coordinate coordinate : coords){
			com.vividsolutions.jts.geom.Point point = gf.createPoint(coordinate);

			if(point.within(queryGeom)){
				finalCoords.add(coordinate);
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("Time taken: " + (end - start));
		System.out.println("Number of candidates: " + finalCoords.size());
	}

	private static Envelope getQuerySpace(Point centroid){
		GeometricShapeFactory gsf = new GeometricShapeFactory();
		gsf.setSize(309);
		gsf.setNumPoints(100);
		gsf.setCentre(new Coordinate(centroid.getX(), centroid.getY()));
		Polygon circle = gsf.createCircle();
		queryGeom = circle.getEnvelope();
		return circle.getEnvelopeInternal();
	}
	
	private static Point getQueryPoint(){
		return new Point(41.956013, -122.440498);
	}

	private static Quadtree getQuadTree(Envelope envelope, Coordinate [] coordinates){
		Quadtree quadTree = new Quadtree();
		for(Coordinate coord : coordinates){
			quadTree.insert(envelope, coord);
		}
		return quadTree;
	}

	private static void runTest(){
		//Populate points
		List<Point> points = PivotTester.populatePointsFromCaliforniaRoadsDataset();
		Coordinate [] coordinates = PivotUtilities.convertPointListToCoordArray(points);
		ConvexHull convexHull = new ConvexHull(PivotUtilities.convertPointListToCoordArray(points), new GeometryFactory());
		Envelope envelope = convexHull.getConvexHull().getEnvelopeInternal();
		//Get populate quadtree
		Quadtree quadtree = getQuadTree(envelope, coordinates);
		//Get query point
		Point queryPoint = getQueryPoint();
		//Get query rectangle
		Envelope queryRectangle = getQuerySpace(queryPoint);
		//Execute query
		queryTree(quadtree, queryRectangle);
	}

	private static void init(){
		runTest();

	}

	public static void main(String [] args){
		init();
	}
}
