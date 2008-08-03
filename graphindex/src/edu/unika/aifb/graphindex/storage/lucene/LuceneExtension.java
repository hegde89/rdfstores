package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.AbstractExtension;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;

public class LuceneExtension extends AbstractExtension {
	public static final class Status extends Parameter {
		private Status(String name) {
			super(name);
		}
		
		public static final Status NEW = new Status("NEW");
		public static final Status STUB = new Status("STUB");
		public static final Status MODIFIED = new Status("MODIFIED");
		public static final Status PART_LOADED = new Status("PART_LOADED");
		public static final Status PERSISTED = new Status("PERSISTED");
		public static final Status REMOVED = new Status("REMOVED");
	}

	private LuceneExtensionStorage m_les;
	private String m_uri;
//	private Status m_status = Status.NEW;
//	private Map<String,Boolean> m_loadStatus;
//	
//	private Map<String,Set<Triple>> m_property2Triple;
	
	public LuceneExtension(String uri, ExtensionManager manager) throws StorageException {
		super(uri, manager);
		
		m_les = (LuceneExtensionStorage)manager.getExtensionStorage();
		
//		m_loadStatus = new HashMap<String,Boolean>();
		
		if (m_manager.extensionExists(m_uri)) {
			if (m_manager.registerExtensionHandler(m_uri, this)) {
//				setStatus(Status.STUB);
			}
			else {
				throw new StorageException("unable to register extension handler");
			}
		}
//		else
//			setStatus(Status.NEW);
	}

	public void unload() {
		if (isCachingOn()) {
			throw new UnsupportedOperationException("LuceneExtension does not support caching");
		}
	}
	
	public Set<Triple> getTriples(String propertyUri) throws StorageException {
		if (isCachingOn()) {
			throw new UnsupportedOperationException("LuceneExtension does not support caching");
//			ensureLoaded(propertyUri, object);
		}
		
		try {
			return m_les.loadData(getUri(), propertyUri);
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public Set<Triple> getTriples(String propertyUri, String object) throws StorageException{
		if (isCachingOn()) {
			throw new UnsupportedOperationException("LuceneExtension does not support caching");
//			ensureLoaded(propertyUri, object);
		}
		
		try {
			return m_les.loadData(getUri(), propertyUri, object);
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
//	private void ensureLoaded(String propertyUri) {
//		Boolean status = m_loadStatus.get(propertyUri);
//		if (status == null || status == false) {
//			try {
//				Set<Triple> triples = m_les.loadData(this.getUri(), propertyUri);
//			} catch (IOException e) {
//				// TODO storage exception
//				e.printStackTrace();
//			}
//		}
//	}
//	
//	private void ensureLoaded(String propertyUri, String object) {
//		
//	}
	
//	private Status getStatus() {
//		return m_status;
//	}
//
//	private void setStatus(Status status) {
//		m_status = status;
//	}
//	
//	private boolean isNew() {
//		return m_status == Status.NEW;
//	}
//	
//	private boolean isModified() {
//		return m_status == Status.MODIFIED;
//	}
//	
//	private boolean isPersisted() {
//		return m_status == Status.PERSISTED;
//	}
	
//	public Set<String> getPropertyUris() {
//		
//	}
	
	public void addTriple(String subject, String property, String object) {
		addTriple(new Triple(subject, property, object));
	}
	
	public void addTriple(Triple triple) {
		if (isCachingOn()) {
			throw new UnsupportedOperationException("LuceneExtension does not support caching");
//			Set<Triple> triples = m_property2Triple.get(triple.getProperty());
//			if (triples == null) {
//				triples = new HashSet<Triple>();
//				m_property2Triple.put(triple.getProperty(), triples);
//			}
//			triples.add(triple);
		}
		m_les.saveData(getUri(), triple);
	}
	
	public void addTriples(Set<Triple> triples) {
		m_les.saveData(getUri(), triples);
	}
	
	public void mergeExtension(Extension extension) {
		
	}
}
