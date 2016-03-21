package usace.army.mil.erdc.pivots.accumulo;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class AccumuloConnectionManager {
	private static ClientOnRequiredTable opts;
	private static Connector connector;

	public AccumuloConnectionManager(ClientOnRequiredTable opts){
		AccumuloConnectionManager.opts = opts;
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

	//Getters and setters
	public Connector getConnector() {
		return connector;
	}

	public void setConnector(Connector connector) {
		AccumuloConnectionManager.connector = connector;
	}


}
