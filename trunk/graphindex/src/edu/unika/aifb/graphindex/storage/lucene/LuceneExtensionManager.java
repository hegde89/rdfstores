package edu.unika.aifb.graphindex.storage.lucene;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
}

