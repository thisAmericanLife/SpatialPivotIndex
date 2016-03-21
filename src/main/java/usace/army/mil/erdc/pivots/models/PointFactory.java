package usace.army.mil.erdc.pivots.models;

public class PointFactory {
	public IPoint getPoint(IPoint.PointType pointType){
		switch(pointType){
		case CANDIDATE:
			return new CandidatePoint();
		case PIVOT:
			return new Pivot();
		case POINT:
			return new Point();
		default:
			return new Point();
		}
	}
	
	public IPoint getPoint(IPoint.PointType pointType, Point point){
		switch(pointType){
		case CANDIDATE:
			return new CandidatePoint(point);
		case PIVOT:
			return new Pivot(point);
		default:
			return new Point();
		}
	}
	
	public IPoint getPoint(IPoint.PointType pointType, Point point, String UID){
		switch(pointType){
		case CANDIDATE:
			return new CandidatePoint(point);
		case PIVOT:
			return new Pivot(point, UID);
		default:
			return new Point();
		}
	}
}
