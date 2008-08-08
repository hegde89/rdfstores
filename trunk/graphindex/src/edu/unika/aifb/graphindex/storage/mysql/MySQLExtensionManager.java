package edu.unika.aifb.graphindex.storage.mysql;

import edu.unika.aifb.graphindex.storage.AbstractExtensionManager;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.StorageException;

public class MySQLExtensionManager extends AbstractExtensionManager {

	public Extension extension(String extUri) throws StorageException {
		if (m_handlers.containsKey(extUri))
			return m_handlers.get(extUri);
		
		MySQLExtension e = new MySQLExtension(extUri, this);
		return e;
	}

	public void flushAllCaches() throws StorageException {
		// TODO Auto-generated method stub
		
	}
}
