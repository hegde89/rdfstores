package edu.unika.aifb.graphindex;

import edu.unika.aifb.graphindex.storage.StorageException;

public interface IndexMerger<G> {
	public boolean merge(G small, G large) throws StorageException;
}
