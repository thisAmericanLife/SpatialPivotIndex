package usace.army.mil.erdc.pivots.accumulo;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class PointMapFilter extends Filter {
	public static final String ROW_ID = "rowID"; 
	public static final String DEFAULT = "";
	private String rowID;
	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		String option = options.containsKey(ROW_ID) ? options.get(ROW_ID) : DEFAULT;
		this.rowID = option;
	}

	@Override
	public boolean accept(Key k, Value v) {
		/*return (k.getRow().toString().equals(rowID) &&
				k.getColumnFamily().toString().equals("MAP") &&
				k.getColumnQualifier().toString().equals("ENTRY"));*/
		Key startKey = new Key(rowID, "MAP", "ENTRY");
		//Key endKey = new Key(new StringBuilder().append(rowID).append("\0").toString(), "MAP", "ENTRY");
		return k.equals(startKey);
		//return (! (k.getColumnFamily().toString().equals("MAP") &&
			//	k.getColumnQualifier().toString().equals("ENTRY")));
	}
}
