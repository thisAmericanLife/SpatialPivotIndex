package usace.army.mil.erdc.Pivots.Utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticFileGenerator {
	//Extent: (-124.848974, 24.396308) - (-66.885444, 49.384358)
	final static private Double MIN_X = -66.885444;
	final static private Double MIN_Y = 24.396308;
	final static private Double MAX_X = -124.848974;
	final static private Double MAX_Y = 49.384358;
	final static private Double DX = 2.0;
	final static private Double DY = 2.0;
	
	private double getX(){
		return ThreadLocalRandom.current().nextDouble(MIN_X, MAX_X + 1);
	}
	
	private double getY(){
		return ThreadLocalRandom.current().nextDouble(MIN_Y, MAX_Y + 1);
	}

	private void generateFile(String fileName, int numPoints){
		try{
			String content = "This is the content to write into file";

			File file = new File(fileName);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			
			for(int i = 0; i < numPoints; i++){
				bw.write(content);
			}
			
			bw.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String [] args){

	}
}
