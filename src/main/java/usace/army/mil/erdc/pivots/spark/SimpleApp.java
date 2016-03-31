package usace.army.mil.erdc.pivots.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;



public class SimpleApp {

	public static void main(String[] args) throws TableNotFoundException {

		String environment = "local";
		System.setProperty("hadoop.home.dir", "/home/ktyler/Documents/spark/hadoop-common-2.2.0-bin-master");
		// System.setProperty("hadoop.home.dir", "/usr/local/hadoop");
		SparkConf sparkConf = new SparkConf().setAppName("Simple Application");
		sparkConf.setMaster("spark://drogba:7077");  

		if (environment.equals("local")) {
			sparkConf.setJars(new String[]{"C:\\Users\\RDTECRS9\\workspace\\strider\\target\\strider-0.0.1-SNAPSHOT.jar"});                       
		} else {
			sparkConf.setJars(new String[]{"/opt/spark-1.6.0-bin-without-hadoop/jars/strider-0.0.1-SNAPSHOT.jar"});
		}
		sparkConf.setExecutorEnv("ACCUMULO_CONF_DIR", "/opt/accumulo-1.7.0/conf");
		sparkConf.setExecutorEnv("SPARK_EXECUTOR_INSTANCES", "6");
		sparkConf.setExecutorEnv("SPARK_EXECUTOR_MEMORY", "1024m");
		sparkConf.setExecutorEnv("SPARK_WORKER_MEMORY", "1024m");
		sparkConf.setExecutorEnv("SPARK_WORKER_CORES", "3");
		sparkConf.setExecutorEnv("HADOOP_CONF_DIR", "/usr/local/hadoop/etc/hadoop");
		sparkConf.setExecutorEnv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.setExecutorEnv("SPARK_WORKER_DIR", "/home/hduser/work");
		//  sparkConf.setExecutorEnv("SPARK_WORKER_TIMEOUT", "30000");
		// sparkConf.setExecutorEnv("SPARK_AKKA_TIMEOUT", "30000");
		// sparkConf.setExecutorEnv("SPARK_LOCAL_DIR", "/home/hduser/work");
		sparkConf.registerKryoClasses(new Class[] {org.apache.accumulo.core.data.Key.class});
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		Connector conn = null;

		Job jobConf = null;

		try {
			jobConf = Job.getInstance(sc.hadoopConfiguration());
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		Authorizations authorizations = new Authorizations();

		try {
			AccumuloInputFormat.setConnectorInfo(jobConf, "root", new PasswordToken("amledd"));
		} catch (AccumuloSecurityException e1) {
			e1.printStackTrace();
		}

		//      AccumuloInputFormat.setScanAuthorizations(jobConf, authorizations);
		ClientConfiguration clientConfig =  new ClientConfiguration().withInstance("strider").withZkHosts("schweinsteiger:2181,neuer:2181,neymar:2181");
		AccumuloInputFormat.setZooKeeperInstance(jobConf, clientConfig);
		String tableName = "points";
		AccumuloInputFormat.setInputTableName(jobConf, tableName);
		AccumuloInputFormat.setScanAuthorizations(jobConf, authorizations);
		//      BatchScanner bscan = null;

		//      String columnRange = null;
		//      String columnQualifier = null;
		//      String columnFamily = null;

		//try {
		ArrayList<Range> myRanges = new ArrayList<Range>();
		myRanges.add(new Range("point_3567","point_3567"));
		//myRanges.add(new Range("11","11"));
		//      AccumuloInputFormat.setRanges(jobConf, myRanges);      
		//                       HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
		//                          columnsToFetch.add(new Pair<Text, Text>(new Text("POINT"),new Text("POJO")));
		//          AccumuloInputFormat.fetchColumns(jobConf, columnsToFetch);

		//      AccumuloInputFormat.fetchColumns(jobConf, new Text(columnFamily), new Text(columnQualifier));

		//      AccumuloInputFormat.setBatchScan(jobConf, true);
		//      bscan = connector.createScanner(tableName, new Authorizations());
		//      Key startKey = new Key(columnRange, columnFamily, columnQualifier);
		//      Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

		//IteratorSetting itr = new IteratorSetting(1, "myIterator", RowFilter.class);
		//itr.addOption(PointMapFilter.ROW_ID, columnRange);

		//scan.addScanIterator(itr);

		//      scan.setRange(new Range(columnRange, new StringBuilder().append(columnRange).append("\0").toString()));
		//      scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		//      } catch (TableNotFoundException e) {
		//              e.printStackTrace();
		//      }






		//============================================

		//      String instanceName = "strider";
		//      String zooServers = "schweinsteiger:2181,neuer:2181,neymar:2181";
		//      Instance inst = new ZooKeeperInstance(instanceName, zooServers);
		//       try {
		//              conn = inst.getConnector("root", "amledd");
		//      } catch (AccumuloException | AccumuloSecurityException e) {
		//             
		//              e.printStackTrace();
		//      }
		//       Text[] terms = new Text[2];
		//        terms[0] = new Text("stadium");
		//        terms[1] = new Text("strike");
		//       
		//       bscan = conn.createBatchScanner("twitter_concepts", authorizations, 10);
		//
		//      IteratorSetting iter = new IteratorSetting(1, "ii", IntersectingIterator.class);
		//      IntersectingIterator.setColumnFamilies(iter, terms);
		//      bscan.addScanIterator(iter);
		//      bscan.setRanges(Collections.singleton(new Range()));
		//
		//      for(Entry<Key,Value> entry : bscan) {
		//          System.out.println(" " + entry.getKey().getColumnQualifier());
		//      }
		//     


		//============================================
		//      WORKS: QUERY FOR ALL WORDS
		//        Text[] terms = new Text[3];
		//        terms[0] = new Text("iran");
		//        terms[1] = new Text("recovers");        
		//        terms[2] = new Text("information");   
		//        //terms[3] = new Text("strong");      
		//        //terms[4] = new Text("message");     

		//       IteratorSetting iter = new IteratorSetting(1, "ii", IntersectingIterator.class);
		//        IntersectingIterator.setColumnFamilies(iter, terms);
		//        AccumuloInputFormat.addIterator(jobConf, iter);
		//=============================================  

		//  AccumuloInputFormat.setBatchScan(jobConf, true);
		//  AccumuloInputFormat.
		//  ArrayList<Range> myRanges = new ArrayList<Range>();
		//      myRanges.add(new Range("3","11"));
		//      myRanges.add(new Range("11","11"));
		//      AccumuloInputFormat.setRanges(jobConf, myRanges);      



		//  for(Entry<Key,Value> entry : bscan) {
		//     System.out.println(" " + entry.getKey().getColumnQualifier());
		// }









		//      myRanges.add(new Range("705884556570836992","705884556570836992"));
		//      Range range1 = Range.prefix("1","a10".toLowerCase());
		//      Range range2 = Range.prefix("1","a0001");
		//      Range range3 = Range.prefix("12","labiduts");

		//      Range range1 = new Range("3");
		//      Range range2 = Range.prefix("15");
		//      Range range3 = Range.prefix("16");
		//      myRanges.add(range1);
		//      myRanges.add(range2);
		//      myRanges.add(range3);

		//myRanges.add(new Range("7"));




		//       HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
		//    columnsToFetch.add(new Pair<Text, Text>(new Text("TW_TEXT"),new Text("TEXT")));
		//    AccumuloInputFormat.fetchColumns(jobConf, columnsToFetch);







		//      Text[] terms = {new Text("GENUINELY"),new Text("GRACIOUSLY")}; 
		//      IteratorSetting iteratorSetting = new IteratorSetting(1, "ii", IntersectingIterator.class);
		//      IntersectingIterator.setColumnFamilies(iteratorSetting, terms);
		//      AccumuloInputFormat.addIterator(jobConf, iteratorSetting);




		//      Collection<Pair<Text,Text>> fetchColumns = Collections.singleton(new Pair<Text,Text>(new Text("TW_TEXT"), new Text("PLACE")));
		//      AccumuloInputFormat.fetchColumns(jobConf, fetchColumns);
		//  ArrayList<Range> myRanges = new ArrayList<Range>();
		//  myRanges.add(new Range("5","5"));

		//      AccumuloInputFormat.fetchColumns(jobConf, fetchColumns);


		//      Text[] terms = { new Text("collective")};
		//
		Collection<Pair<Text,Text>> fetchColumns = Collections.singleton(new Pair<Text,Text>(new Text("POINT"),null));
		AccumuloInputFormat.fetchColumns(jobConf, fetchColumns);
		//              IteratorSetting iter = new IteratorSetting(1, IntersectingIterator.class);
		//      IntersectingIterator.setColumnFamilies(iter, terms);
		//      AccumuloInputFormat.addIterator(jobConf, iter);

		//IteratorSetting iteratorSetting = new IteratorSetting(1,WholeRowIterator.class);

		//AccumuloInputFormat.addIterator(jobConf, iteratorSetting);


		JavaPairRDD<Key, Value> rdd2 = sc.newAPIHadoopRDD(jobConf.getConfiguration(), AccumuloInputFormat.class, Key.class, Value.class);

		long xdsdfsd = rdd2.count();

		Key k = rdd2.first()._1();
		Value v = rdd2.first()._2();


		//long count = rdd2.count();
		//      tableName = "pointsIndex";
		//      AccumuloInputFormat.setInputTableName(jobConf, tableName);     

		//      List<Key> keys = rdd2.keys().distinct().collect();
		//      for (Key key : keys) {
		//              String rowId = key.getRow().toString();
		//               myRanges = new ArrayList<Range>();
		//               myRanges.add(new Range(new StringBuilder().append("pivot_1").append(rowId).toString()
		//                               ,new StringBuilder().append("pivot_1").append(rowId).toString()));
		//myRanges.add(new Range("11","11"));
		//      AccumuloInputFormat.setRanges(jobConf, myRanges);      
		//       columnsToFetch = new HashSet<Pair<Text,Text>>();
		//          columnsToFetch.add(new Pair<Text, Text>(new Text("MAP"),new Text("ENTRY")));
		//    AccumuloInputFormat.fetchColumns(jobConf, columnsToFetch);
		//      AccumuloInputFormat.fetchColumns(jobConf, new Text(columnFamily), new Text(columnQualifier));
		//AccumuloInputFormat.setBatchScan(jobConf, true);
		//      JavaPairRDD<Key, Value> rdd3
		//      = sc.newAPIHadoopRDD(jobConf.getConfiguration(), AccumuloInputFormat.class, Key.class, Value.class);
		//      Value value = rdd3.first()._2();


	}



	//      Text row =      key.getRow();
	//      System.out.println(key.getRow().toString() + " ");
	//      System.out.print(key.getColumnFamily() + " ");
	//      System.out.print(key.getColumnQualifier() + " ");
	//      System.out.print(key.getTimestamp() + " ");    
	//      List<Value> list = rdd2.values().distinct().collect();
	//      for (Value value : list) {

	///     }




	// Text t = k.getRow();
	// System.out.println(k.getColumnFamily());
	// System.out.println(v.toString());
	// }

	//public static Scanner queryAccumuloWithStartAndEndKey(String tableName, String columnRange,
	//              String columnFamily, String columnQualifier){
	//      Scanner scan = null;
	//      try {
	//              scan = connector.createScanner(tableName, new Authorizations());
	//              Key startKey = new Key(columnRange, columnFamily, columnQualifier);
	//              Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);
	//             
	//              //IteratorSetting itr = new IteratorSetting(1, "myIterator", RowFilter.class);
	//              //itr.addOption(PointMapFilter.ROW_ID, columnRange);
	//             
	//              //scan.addScanIterator(itr);
	//             
	//              scan.setRange(new Range(columnRange, new StringBuilder().append(columnRange).append("\0").toString()));
	//              scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
	//      } catch (TableNotFoundException e) {
	//              e.printStackTrace();
	//      }
	//      return scan;
	//}
	//
	//
	//
	//
	//
	//private double getPrecomputedDistanceFromAccumulo(Point point, Pivot pivot){
	//      double distance = 0.0;
	//      Scanner scanner = AccumuloConnectionManager.queryAccumuloWithStartAndEndKey(POINT_INDEX_TABLE_NAME,
	//                      getPivotToPointID(pivot.getPivotID(), point.getUID()),
	//                      "MAP", "ENTRY");
	//      for(Entry<Key,Value> scannerEntry : scanner) {
	//             
	//              PivotMapEntry mapEntry = gson.fromJson(scannerEntry.getValue().toString(), PivotMapEntry.class);
	//              //Verify that we have the map entry for the correct point
	//              //if(mapEntry.getPointID().equals(point.getUID())){
	//                      distance = mapEntry.getDistance();
	//                      break;
	//              //}
	//      }
	//      return distance;
}