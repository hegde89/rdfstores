package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.storage.AbstractExtension;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;

public class LuceneExtension extends AbstractExtension {

	private LuceneExtensionStorage m_les;
	private Map<String,Set<Triple>> m_readCache;
	private Set<Triple> m_writeCache;
	
	private final static Logger log = Logger.getLogger(LuceneExtension.class);
	
	public LuceneExtension(String uri, ExtensionManager manager) throws StorageException {
		super(uri, manager);
		
		m_les = (LuceneExtensionStorage)manager.getExtensionStorage();
	
		m_readCache = new HashMap<String,Set<Triple>>();
		m_writeCache = new HashSet<Triple>();
		
		if (m_manager.extensionExists(getUri())) {
			if (m_manager.registerExtensionHandler(getUri(), this)) {
			}
			else {
				throw new StorageException("unable to register extension handler");
			}
		}
		else {
			m_manager.registerExtensionHandler(getUri(), this);
		}
	}

	public void flush() throws StorageException {
		try {
			switch (getMode()) {
			case ExtensionManager.MODE_NOCACHE:
				break;
				
			case ExtensionManager.MODE_WRITECACHE:
				if (m_writeCache.size() == 0)
					return;

				Set<Triple> storedTriples = m_les.loadData(getUri());
//				log.debug(getUri() + ": st: " + storedTriples.size() + ", cache: " + m_writeCache.size());
				m_writeCache.addAll(storedTriples);
//				log.debug(getUri() + ": " + m_writeCache.size());
				
				m_les.deleteData(getUri());
//				log.debug("after delete: " + m_les.loadData(getUri()).size());
				m_les.saveData(getUri(), m_writeCache);

				m_writeCache.clear();
				break;
				
			case ExtensionManager.MODE_READONLY:
				m_readCache.clear();
				break;
				
			default:
				throw new UnsupportedOperationException("unknown mode");	
			}
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void remove() throws StorageException {
		try {
			switch (getMode()) {
			case ExtensionManager.MODE_NOCACHE:
			case ExtensionManager.MODE_WRITECACHE:
				m_les.deleteData(getUri());
				break;
				
			case ExtensionManager.MODE_READONLY:
				throw new UnsupportedOperationException("read only mode: removing extensions not permitted");	
			}
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public Set<Triple> getTriples() throws StorageException {
		if (getMode() == ExtensionManager.MODE_WRITECACHE)
			return m_writeCache;
		return getTriples(null, null);
	}
	
	public Set<Triple> getTriples(String propertyUri) throws StorageException {
		return getTriples(propertyUri, null);
	}
	
	public Set<Triple> getTriples(String propertyUri, String object) throws StorageException{
		Set<Triple> triples;
		try {
			switch (getMode()) {
			case ExtensionManager.MODE_NOCACHE:
				if (propertyUri == null)
					triples = m_les.loadData(getUri());
				else
					triples = object == null ? m_les.loadData(getUri(), propertyUri) : m_les.loadData(getUri(), propertyUri, object);
				break;
				
			case ExtensionManager.MODE_WRITECACHE:
				throw new UnsupportedOperationException("write only mode: reads not permitted");
				
			case ExtensionManager.MODE_READONLY:
				String cacheKey = null;
				if (propertyUri != null)
					cacheKey = object == null ? propertyUri : propertyUri + "|" + object;
				
				triples = m_readCache.get(cacheKey);
				if (triples == null) {
					if (propertyUri == null)
						triples = m_les.loadData(getUri());
					else
						triples = object == null ? m_les.loadData(getUri(), propertyUri) : m_les.loadData(getUri(), propertyUri, object);
					m_readCache.put(cacheKey, triples);
				}
				break;
				
			default:
				throw new UnsupportedOperationException("unknown mode");	
			}
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
		
		return triples;
	}
	
	public void addTriple(String subject, String property, String object) {
		addTriple(new Triple(subject, property, object));
	}
	
	public void addTriple(Triple triple) {
		switch (getMode()) {
		case ExtensionManager.MODE_NOCACHE:
			m_les.saveData(getUri(), triple);
			break;
			
		case ExtensionManager.MODE_WRITECACHE:
			m_writeCache.add(triple);
			break;
			
		case ExtensionManager.MODE_READONLY:
			throw new UnsupportedOperationException("read only mode: adding triples not permitted");
		}
	}
	
	public void addTriples(Set<Triple> triples) {
		switch (getMode()) {
		case ExtensionManager.MODE_NOCACHE:
			m_les.saveData(getUri(), triples);
			break;
			
		case ExtensionManager.MODE_WRITECACHE:
			m_writeCache.addAll(triples);
			break;
			
		case ExtensionManager.MODE_READONLY:
			throw new UnsupportedOperationException("read only mode: adding triples not permitted");
		}
	}
	
	public void mergeExtension(Extension extension) throws StorageException {
//		log.debug("merging " + extension.getUri() + " into " + getUri());
		try {
			if (getMode() == ExtensionManager.MODE_READONLY)
				throw new UnsupportedOperationException("read only mode: merging not permitted");
			
			Set<Triple> ownTriples, otherTriples;
			
			if (getMode() == ExtensionManager.MODE_NOCACHE) {
				ownTriples = getTriples();
				otherTriples = extension.getTriples();
			}
			else if (getMode() == ExtensionManager.MODE_WRITECACHE) {
				ownTriples = m_les.loadData(getUri());
				ownTriples.addAll(m_writeCache);
				
				otherTriples = m_les.loadData(extension.getUri());
				otherTriples.addAll(extension.getTriples());
			}
			else {
				throw new UnsupportedOperationException("unknown mode: " + getMode());
			}
			
//			log.debug("own: " + ownTriples.size());
//			log.debug("other: " + otherTriples.size());
			
			ownTriples.addAll(otherTriples);
			
//			log.debug("after: " + ownTriples.size());
			
			if (getMode() == ExtensionManager.MODE_WRITECACHE)
				m_writeCache = ownTriples;
			else {
				m_les.deleteData(getUri());
				m_les.saveData(getUri(), ownTriples);
			}
			
			m_manager.removeExtension(extension.getUri());
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
}
