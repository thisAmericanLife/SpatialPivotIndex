package usace.army.mil.erdc.pivots.accumulo;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import com.google.gson.Gson;

import usace.army.mil.erdc.pivots.models.Pivot;
import usace.army.mil.erdc.pivots.models.Point;

public class ScannerTester {
	private static final String INSTANCE_NAME = "strider";
	private static final String ZOOKEEPERS = "schweinsteiger:2181,neuer:2181,neymar:2181";
	private static final String ACCUMULO_USER = "root";
	private static final String ACCUMULO_PASSWORD = "amledd";
	private static Connector getConnector(){
		Instance instance  = new ZooKeeperInstance(INSTANCE_NAME, ZOOKEEPERS);
		Connector connector = null;
		try {
			connector = instance.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
		} catch (AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}
		return connector;
	}

	public static void main(String [] args){
		try {
			Gson gson = new Gson();
			Pivot pivot = null;
			Connector connector = getConnector();
			Scanner points = AccumuloConnectionManager.queryAccumulo("points", "POINT", "POJO", connector);
			Scanner pivots = AccumuloConnectionManager.queryAccumulo("points", "PIVOT", "POJO", connector);

			for(Entry<Key,Value> pivotEntry : pivots) {
				pivot = gson.fromJson(pivotEntry.getValue().toString(), Pivot.class);
				break;
			}
			long start = System.currentTimeMillis();
			String rowIDPrefix = "pivot_0_";
			for(Entry<Key,Value> neighborEntry : points) {
				Point point = gson.fromJson(neighborEntry.getValue().toString(), Point.class);
				double distance = 0.0;
				Scanner scanner;
				String rowID = new String(rowIDPrefix.concat(point.getUID()));
				//rowID += point.getUID();
				
				String hashedRow = String.valueOf(rowID.hashCode());
				//System.out.println(rowID);
				//System.out.println(hashedRow);
				scanner = AccumuloConnectionManager.queryAccumuloWithFilter("pointsIndex", 
						hashedRow,
						point.getUID(), "", connector);
				for(Entry<Key,Value> scannerEntry : scanner) {
					//Verify that we have the map entry for the correct point
					//if(scannerEntry.getKey().getRow().toString()
					//	.equals(pivot.getPivotID())){
					distance = Double.parseDouble(scannerEntry.getKey().getColumnQualifier().toString());
					//System.out.println(distance);
					break;
					//}
				}
			}
			/*//AccumuloConnectionManager.setConnector(connector);
		//	Scanner newScanner = connector.createScanner("pointsIndex", new Authorizations());
			BatchScanner newBatchScanner = connector.createBatchScanner("pointsIndex", new Authorizations(), 60);
			for(int i = 0 ; i < 21048; i ++){
			//	Scanner scanner = AccumuloConnectionManager.queryAccumuloWithFilter2("pointsIndex", 
				//		"[B@10002cd",
					//	"point_20866", "2.3033514168532867");
				
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range("[B@10002cd"));
				rangeList.add(new Range( "[B@10002cd"));
				newBatchScanner.setRanges(rangeList);
				newBatchScanner.fetchColumnFamily(new Text("point_20866"));
				for(Entry<Key,Value> scannerEntry : newBatchScanner) {
					//System.out.println(scannerEntry.getKey().getColumnQualifier().toString());
					double distance = Double.parseDouble(scannerEntry.getKey().getColumnQualifier().toString());
					//System.out.println(distance);
					break;
				}
			}*/
			long end = System.currentTimeMillis();
			System.out.println("Time taken for loop: " + (end - start));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
