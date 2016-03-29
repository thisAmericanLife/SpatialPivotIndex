package usace.army.mil.erdc.pivots.accumulo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import usace.army.mil.erdc.pivots.accumulo.ModifiedGrepIterator;

public class AccumuloConnectionManager {
	private static ClientOnRequiredTable opts;
	private static Connector connector;
	private static final int NUM_TABLET_SERVERS = 5;
	private static final String PIVOT_PREFIX = "pivot_";

	public AccumuloConnectionManager(ClientOnRequiredTable opts){
		AccumuloConnectionManager.opts = opts;
	}

	/*public static void addSplitsBasedOnPivots(String tableName, int numPivots){
		try{
			SortedSet<Text> partitionKeys = new TreeSet<Text>();
			for(int i = 0; i < numPivots; i++){
				partitionKeys.add(new Text(new StringBuilder().append(PIVOT_PREFIX).append(String.valueOf(i)).toString()));
				if(i == (NUM_TABLET_SERVERS -1)){
					break;
				}
			}
			connector.tableOperations().addSplits(tableName, partitionKeys);
		} catch(TableNotFoundException | AccumuloException | AccumuloSecurityException e){
			e.printStackTrace();
		}
	}*/

	public static void prepareTablesForTest(List<String> tables){
		for(String table : tables){
			try {
				//If Table exists, blow that bad boy away
				if(connector.tableOperations().exists(table)){
					connector.tableOperations().delete(table);
					connector.tableOperations().create(table);
				} else{
					//If not, then create new
					connector.tableOperations().create(table);
				}
			} catch (AccumuloException | AccumuloSecurityException | TableExistsException
					| TableNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void verifyTableExistence(String tableName){
		if(!connector.tableOperations().exists(tableName))
		{
			try {
				connector.tableOperations().create(tableName);
			} catch (AccumuloException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AccumuloSecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TableExistsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void connect(){
		setConnector(null);
		try {
			setConnector(opts.getConnector());
		} catch (AccumuloException | AccumuloSecurityException ex) {
			ex.printStackTrace();
		}
	}

	public static BatchScanner getBatchScanner(String tableName){
		BatchScanner batchScanner = null;
		try {
			batchScanner= connector.createBatchScanner(tableName, new Authorizations(), 10);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return batchScanner;
	}

	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly.
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumulo(String tableName, Text prefix,
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			scan.setRange(Range.prefix(prefix));
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	public static Scanner queryAccumuloWithFilter(String tableName, 
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			/*IteratorSetting itr = new IteratorSetting(15, "myIterator", PointMapFilter.class);
			itr.addOption(PointMapFilter.ROW_ID, rowID);
			scan.addScanIterator(itr);*/


			scan.setRange(new Range());
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));


		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly.
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumuloWithStartAndEndKey(String tableName, String columnRange,
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			Key startKey = new Key(columnRange, columnFamily, columnQualifier);
			Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

			//  IteratorSetting iteratorSetting = new IteratorSetting(1, WholeRowIterator.class);
			//scan.addScanIterator(iteratorSetting);

			scan.setRange(new Range(columnRange, columnRange));
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	public static BatchScanner queryAccumuloWithIterator(String tableName, String columnRange,
			String columnFamily, String columnQualifier){
		BatchScanner scan = null;
		try {
			scan = connector.createBatchScanner(tableName, new Authorizations(), 10);
			Key startKey = new Key(columnRange, columnFamily, columnQualifier);
			Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

			Text[] terms = new Text[1];
			terms[0] = new Text(columnFamily);
			IteratorSetting iterSetting = new IteratorSetting(1, "IntersectingIterator", IntersectingIterator.class);
			IntersectingIterator.setColumnFamilies(iterSetting, terms);



			/*Map<String,String> grepProps = new HashMap<String,String>();
			grepProps.put("term", columnRange);
			IteratorSetting iteratorSetting = new IteratorSetting(1, ModifiedGrepIterator.class, grepProps);*/
			scan.addScanIterator(iterSetting);
			//List<Range> ranges = new ArrayList<Range>();
			//ranges.add(new Range(columnRange, columnRange));
			//scan.setRanges(ranges);
			//scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
			scan.setRanges(Collections.singleton(new Range(columnRange, columnRange)));

		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}
	
	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly.
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumuloWithFilter(String tableName, String columnRange,
			String columnFamily, String columnQualifier, Connector connection){
		Scanner scan = null;
		try {
			scan = connection.createScanner(tableName, new Authorizations());
			/*Key startKey = new Key(columnRange, 
					columnFamily, 
					columnQualifier);
			Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

			IteratorSetting itr = new IteratorSetting(1, "myIterator", PointMapFilter.class);
			itr.addOption(PointMapFilter.ROW_ID, columnRange);

			scan.addScanIterator(itr);*/

			scan.setRange(new Range(columnRange, columnRange));
			scan.fetchColumnFamily(new Text(columnFamily));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}
	public static Scanner queryAccumuloWithFilter2(String tableName, String columnRange,
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			/*Key startKey = new Key(columnRange, 
					columnFamily, 
					columnQualifier);
			Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

			IteratorSetting itr = new IteratorSetting(1, "myIterator", PointMapFilter.class);
			itr.addOption(PointMapFilter.ROW_ID, columnRange);

			scan.addScanIterator(itr);*/

			scan.setRange(new Range(columnRange, columnRange));
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly.
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumuloWithFilter(String tableName, String columnRange,
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			Key startKey = new Key(columnRange, columnFamily, columnQualifier);
			Key endKey = new Key(new StringBuilder().append(columnRange).append("\0").toString(), columnFamily, columnQualifier);

			/*IteratorSetting itr = new IteratorSetting(1, "myIterator", PointMapFilter.class);
			itr.addOption(PointMapFilter.ROW_ID, columnRange);

			scan.addScanIterator(itr);*/

			scan.setRange(new Range(startKey, endKey));
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}
	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly.
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumulo(String tableName, String columnRange,
			String columnFamily, String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			scan.setRange(new Range(columnRange));
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}
	
	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly- overloaded to omit columnRange;
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumulo(String tableName, String columnFamily, 
			String columnQualifier, Connector connection){
		Scanner scan = null;
		try {
			scan = connection.createScanner(tableName, new Authorizations());
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	/**
	 * Returns scanner object for a given query criteria, and allows calling function to iterate
	 * over the results accordingly- overloaded to omit columnRange;
	 * @param tableName
	 * @param columnRange
	 * @param columnFamily
	 * @param columnQualifier
	 * @return
	 */
	public static Scanner queryAccumulo(String tableName, String columnFamily, 
			String columnQualifier){
		Scanner scan = null;
		try {
			scan = connector.createScanner(tableName, new Authorizations());
			scan.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		return scan;
	}

	/**
	 * Returns mutation objects to be appended to BatchWriter.
	 * @param rowID
	 * @param columnFamily
	 * @param columnQualifer
	 * @param value
	 * @return
	 */
	public static Mutation getMutation(String rowID, String columnFamily, String columnQualifer,
			String value){
		ColumnVisibility colVis = new ColumnVisibility();
		long timestamp = System.currentTimeMillis();

		Mutation mutation = new Mutation(new Text(rowID));
		mutation.put(new Text(columnFamily), new Text(columnQualifer),
				colVis, timestamp, new Value(value.getBytes()));
		return mutation;
	}



	//Takes list of Mutations as input, adds them to memory 
	//      efficient batch writer from table/instance configurations
	//      provided in main(), then wrties to Accumulo
	public static void writeMutations(List<Mutation> mutations, BatchWriterOpts bwOpts, BatchWriterConfig bwConfig, boolean flush){
		try{
			BatchWriter writer =
					connector.createBatchWriter(opts.getTableName(), 
							bwConfig);
			for(Mutation mutation : mutations){
				writer.addMutation(mutation);
			}
			if(flush){
				writer.flush();
			} else{
				writer.close(); //This performs the flush and actual write to HDFS
			}
		} catch (MutationsRejectedException | TableNotFoundException ex){
			ex.printStackTrace();
		}
	}

	public static void writeMutations(List<Mutation> mutations, String tableName, BatchWriterConfig bwConfig){
		try{
			BatchWriter writer = connector.createBatchWriter(tableName, bwConfig);
			for(Mutation mutation: mutations){
				writer.addMutation(mutation);
			}
			writer.close();
		} catch (MutationsRejectedException | TableNotFoundException ex){
			ex.printStackTrace();
		}
	}

	//Takes list of Mutations as input, adds them to memory 
	//      efficient batch writer from table/instance configurations
	//      provided in main(), then wrties to Accumulo
	public static void writeMutations(List<Mutation> mutations, BatchWriterOpts bwOpts, BatchWriterConfig bwConfig){
		try{
			BatchWriter writer =
					connector.createBatchWriter(opts.getTableName(), 
							bwConfig);
			for(Mutation mutation : mutations){
				writer.addMutation(mutation);
			}
			writer.close(); //This performs the flush and actual write to HDFS

		} catch (MutationsRejectedException | TableNotFoundException ex){
			ex.printStackTrace();
		}
	}

	//Takes list of Mutations as input, adds them to memory 
	//      efficient batch writer from table/instance configurations
	//      provided in main(), then wrties to Accumulo
	public static void writeMutations(List<Mutation> mutations, BatchWriterOpts bwOpts){
		try{
			BatchWriter writer =
					connector.createBatchWriter(opts.getTableName(), 
							bwOpts.getBatchWriterConfig());
			for(Mutation mutation : mutations){
				writer.addMutation(mutation);
			}
			writer.close(); //This performs the flush and actual write to HDFS
		} catch (MutationsRejectedException | TableNotFoundException ex){
			ex.printStackTrace();
		}
	}

	//Get writer to manager flushing from buffer
	public static BatchWriter getBatchWriter(BatchWriterOpts bwOpts, BatchWriterConfig bwConfig){
		BatchWriter batchWriter = null;
		try {
			batchWriter=  connector.createBatchWriter(opts.getTableName(), bwConfig);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return batchWriter;
	}

	//Getters and setters
	public Connector getConnector() {
		return connector;
	}

	public static void setConnector(Connector connector) {
		AccumuloConnectionManager.connector = connector;
	}
}
