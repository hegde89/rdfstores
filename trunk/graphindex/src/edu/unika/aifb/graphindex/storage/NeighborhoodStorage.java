package edu.unika.aifb.graphindex.storage;

import edu.unika.aifb.keywordsearch.index.BloomFilter;

public interface NeighborhoodStorage {
	public BloomFilter getNeighborhoodBloomFilter(String uri) throws StorageException;
	public void close() throws StorageException;
}
