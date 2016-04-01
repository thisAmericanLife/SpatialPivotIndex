package usace.army.mil.erdc.Pivots.models;

public class Quadrant {
	private int numObservations;
	private double area;
	private double density = 0.0;
	
	//Constructors
	public Quadrant(double area){
		this.area = area;
	}
	
	public double getArea() {
		return area;
	}
	public void setArea(double area) {
		this.area = area;
	}
	public void incrememntObservations(){
		numObservations++;
	}
	public int getNumObservations() {
		return numObservations;
	}
	public void setNumObservations(int numObservations) {
		this.numObservations = numObservations;
	}
	public double calculateDensity() {
		return numObservations / area;
	}
	public double getDensity(){
		return density;
	}
	public void setDensity(double density) {
		this.density = density;
	}
}
