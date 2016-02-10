package usace.army.mil.erdc.pivots.accumulo;

import java.util.List;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.beust.jcommander.Parameter;

import usace.army.mil.erdc.pivots.PivotTester;
import usace.army.mil.erdc.pivots.models.IPivotIndex;
import usace.army.mil.erdc.pivots.models.PivotIndexFactory;

public class AccumuloPivotTester extends PivotTester {
	
	//private static ClientOnRequiredTable opts = null;
	private static AccumuloConnectionManager connectionManager = null;
	private static ClientOnRequiredTable opts = null;
    private static BatchWriterOpts bwOpts = null;

	
    //Returns mutation object
    private static Mutation getMutation(String rowID, String columnFamily, String columnQualifer,
                    String value){
                    ColumnVisibility colVis = new ColumnVisibility();
                    long timestamp = System.currentTimeMillis();

                    Mutation mutation = new Mutation(new Text(rowID));
                    mutation.put(new Text(columnFamily), new Text(columnQualifer),
                                    colVis, timestamp, new Value(value.getBytes()));
                    return mutation;
    }
	
    private static void init(String [] args){
		opts = new ClientOnRequiredTable();
		bwOpts = new BatchWriterOpts();
		opts.parseArgs(AccumuloPivotTester.class.getName(), args, bwOpts);
		connectionManager = new AccumuloConnectionManager(opts);
		connectionManager.connect();
		
		PivotIndexFactory indexFactory = new PivotIndexFactory();
		IPivotIndex pivotIndex = indexFactory.getIndex(IPivotIndex.PivotIndexType.ACCUMULO);
	}

	public static void main(String [] args){
		init(args);
	}
	
	static class Opts extends ClientOnRequiredTable {
		@Parameter(names = "--instance")
		String instance = "strider";
		@Parameter(names = "--zookeepers")
		String zookeepers = "schweinsteiger:2181";
		@Parameter(names = "--username")
		String username = "root";
		@Parameter(names = "--password")
		String password = "amledd";
		@Parameter(names = "--table")
		String table = "tweets";
	}
}
