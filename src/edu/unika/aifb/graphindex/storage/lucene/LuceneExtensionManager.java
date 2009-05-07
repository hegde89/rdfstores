package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;

import edu.unika.aifb.graphindex.storage.AbstractExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneExtensionManager extends AbstractExtensionManager {
	public LuceneExtensionManager() {
		super();
	}
	
	public void flushAllCaches() throws StorageException {
		boolean bulk = bulkUpdating();
		if (bulk)
			finishBulkUpdate();
		
		try {
			((LuceneExtensionStorage)m_storage).flushWriter();
		} catch (CorruptIndexException e1) {
			throw new StorageException(e1);
		} catch (IOException e1) {
			throw new StorageException(e1);
		}
		
		if (bulk)
			startBulkUpdate();
	}
}

