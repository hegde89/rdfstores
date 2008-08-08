package edu.unika.aifb.graphindex.storage.mysql;

import java.sql.SQLException;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.AbstractExtension;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;

public class MySQLExtension extends AbstractExtension {

	private MySQLExtensionStorage m_storage;

	public MySQLExtension(String uri, ExtensionManager manager) throws StorageException {
		super(uri, manager);
		
		m_storage = (MySQLExtensionStorage)manager.getExtensionStorage();
		
		if (m_manager.extensionExists(m_uri)) {
			if (m_manager.registerExtensionHandler(m_uri, this)) {
//				setStatus(Status.STUB);
			}
			else {
				throw new StorageException("unable to register extension handler");
			}
		}
		else {
			m_manager.registerExtensionHandler(m_uri, this);
		}
	}

	public void flush() throws StorageException {
	}

	public void remove() throws StorageException {
		
	}
	
	public void addTriple(String subject, String property, String object) throws StorageException {
		addTriple(new Triple(subject, property, object));
	}

	public void addTriple(Triple triple) throws StorageException {
		try {
			m_storage.saveData(getUri(), triple);
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	public void addTriples(Set<Triple> triples) throws StorageException {
		try {
			m_storage.saveData(getUri(), triples);
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}
	
	public Set<Triple> getTriples() throws StorageException {
		return null;
	}

	public Set<Triple> getTriples(String propertyUri) throws StorageException {
		try {
			return m_storage.loadData(getUri(), propertyUri);
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	public Set<Triple> getTriples(String propertyUri, String objectValue) throws StorageException {
		try {
			return m_storage.loadData(getUri(), propertyUri, objectValue);
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	public void mergeExtension(Extension extension) throws StorageException {
	}
}
