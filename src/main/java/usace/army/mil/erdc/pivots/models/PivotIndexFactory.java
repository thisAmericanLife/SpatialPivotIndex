package usace.army.mil.erdc.pivots.models;

import usace.army.mil.erdc.pivots.PivotIndex;
import usace.army.mil.erdc.pivots.accumulo.AccumuloPivotIndex;

public class PivotIndexFactory {
	public IIndexingScheme getIndex(IIndexingScheme.PivotIndexType indexType){
		switch(indexType){
		case SINGLE_NODE:
			return new PivotIndex();
		case ACCUMULO:
			return new AccumuloPivotIndex();
		default:
			return new PivotIndex();
		}
	}
}
