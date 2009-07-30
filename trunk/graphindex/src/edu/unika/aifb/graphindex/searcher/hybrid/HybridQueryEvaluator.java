package edu.unika.aifb.graphindex.searcher.hybrid;

import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.Searcher;

public abstract class HybridQueryEvaluator extends Searcher {

	protected HybridQueryEvaluator(IndexReader idxReader) {
		super(idxReader);
	}

}
