package usace.army.mil.erdc.Pivots.Utilities;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticFileGenerator {
	//Extent: (-124.848974, 24.396308) - (-66.885444, 49.384358)
	final static private Double MIN_X = 66.885444;
	final static private Double MIN_Y = 24.396308;
	final static private Double MAX_X = 124.848974;
	final static private Double MAX_Y = 49.384358;
	final static private Double DX = 2.0;
	final static private Double DY = 2.0;
	
	private static double getX(){
		return ThreadLocalRandom.current().nextDouble(MIN_X, MAX_X + 1) * -1.0;
	}
	
	private static double getY(){
		return ThreadLocalRandom.current().nextDouble(MIN_Y, MAX_Y + 1);
	}

	private static void generateFile(String fileName, int numPoints){
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
				bw.write(new StringBuilder().append(i).append(" ")
						.append(getX()).append(" ")
						.append(getY()).append("\n").toString());
			}
			
			bw.close();

			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String [] args){
		generateFile("/home/ktyler/Documents/strider/synthetic_data/5k.txt", 2500);
		generateFile("/home/ktyler/Documents/strider/synthetic_data/5k.txt", 5000);
		generateFile("/home/ktyler/Documents/strider/synthetic_data/10k.txt", 10000);
		generateFile("/home/ktyler/Documents/strider/synthetic_data/50k.txt", 50000);
		generateFile("/home/ktyler/Documents/strider/synthetic_data/70k.txt", 70000);
		generateFile("/home/ktyler/Documents/strider/synthetic_data/90k.txt", 90000);
	}
}
