package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;

import edu.unika.aifb.graphindex.storage.AbstractExtensionManager;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.StorageException;

public class LuceneExtensionManager extends AbstractExtensionManager {
	public LuceneExtensionManager() {
		super();
	}
	
	public Extension extension(String extUri) throws StorageException {
		if (m_handlers.containsKey(extUri))
			return m_handlers.get(extUri);
		
		LuceneExtension e = new LuceneExtension(extUri, this);
		return e;
	}
	
	public void join(String leftExt, String leftProperty, String rightExt) {
		
	}

	public void flushAllCaches() throws StorageException {
		boolean bulk = bulkUpdating();
		if (bulk)
			finishBulkUpdate();
		
		for (Extension e: m_handlers.values())
			e.flush();
		System.gc();
		
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

