package usace.army.mil.erdc.pivots.accumulo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.GrepIterator;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ModifiedGrepIterator extends Filter {
	
	private byte term[];
	private int right[] = new int[256];

	  @Override
	  public boolean accept(Key k, Value v) {
	    return match(k.getRowData());
	  }

	  protected boolean match(ByteSequence bs) {
	    return indexOf(bs.getBackingArray(), bs.offset(), bs.length()) >= 0;
	  }

	  protected boolean match(byte[] ba) {
	    return indexOf(ba, 0, ba.length) >= 0;
	  }

	  protected int indexOf(byte[] value, int offset, int length) {
	    final int M = term.length;
	    final int N = offset + length;
	    int skip;
	    for (int i = offset; i <= N - M; i += skip) {
	      skip = 0;
	      for (int j = M - 1; j >= 0; j--) {
	        if (term[j] != value[i + j]) {
	          skip = Math.max(1, j - right[value[i + j] & 0xff]);
	        }
	      }
	      if (skip == 0) {
	        return i;
	      }
	    }
	    return -1;
	  }

	  @Override
	  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
		  ModifiedGrepIterator copy = (ModifiedGrepIterator) super.deepCopy(env);
	    copy.term = Arrays.copyOf(term, term.length);
	    return copy;
	  }

	  @Override
	  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
	    super.init(source, options, env);
	    term = options.get("term").getBytes(UTF_8);
	    for (int i = 0; i < right.length; i++) {
	      right[i] = -1;
	    }
	    for (int i = 0; i < term.length; i++) {
	      right[term[i] & 0xff] = i;
	    }
	  }

	  /**
	   * Encode the grep term as an option for a ScanIterator
	   */
	  public static void setTerm(IteratorSetting cfg, String term) {
	    cfg.addOption("term", term);
	  }
}
