package edu.unika.aifb.graphindex.indexing;

import edu.unika.aifb.graphindex.storage.StorageException;

public interface IndexGraphMerger<G> {
	
	/**
	 * This method is called by MergedIndexList to check if two structure index graphs can be merged
	 * and, if this is the case, to merge the graphs.
	 * 
	 * @param small the graph to be merged
	 * @param large the graph, small is to be merged into
	 * @return true if the graphs were merged, false otherwise
	 * @throws StorageException
	 */
	public boolean merge(G small, G large) throws StorageException;
}
