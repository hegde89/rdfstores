package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.data.ExtensionSegment;
import edu.unika.aifb.graphindex.data.SetExtensionSegment;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.storage.AbstractExtension;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class LuceneExtension extends AbstractExtension {

	private LuceneExtensionStorage m_les;
	
	private final static Logger log = Logger.getLogger(LuceneExtension.class);
	
	public LuceneExtension(String uri, ExtensionManager manager) throws StorageException {
		super(uri, manager);
		
		m_les = (LuceneExtensionStorage)manager.getExtensionStorage();
	
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
	}
	
	public void remove() throws StorageException {
		try {
			switch (getMode()) {
			case ExtensionManager.MODE_NOCACHE:
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
	
	public List<Triple> getTriplesList(String propertyUri, String object) throws StorageException {
		try {
			ExtensionSegment es = m_les.loadExtensionSegment(getUri(), propertyUri, object);
			if (es != null)
				return es.toTriples();
			else
				return new ArrayList<Triple>();
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public List<Triple> getTriplesList(String propertyUri) throws StorageException {
		try {
			List<Triple> triples = new ArrayList<Triple>();
			List<ExtensionSegment> ess = m_les.loadExtensionSegments(getUri(), propertyUri);

			long start = System.currentTimeMillis();
			
			if (ess.size() == 1)
				triples = ess.get(0).toTriples();
			else 
				for (ExtensionSegment es : ess)
					triples.addAll(es.toTriples());
			
//			log.debug(System.currentTimeMillis() - start);
			return triples;
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
	public void addTriples(Set<String> subjects, String property, String object) throws StorageException {
		try {
			switch (getMode()) {
			case ExtensionManager.MODE_NOCACHE:
				SetExtensionSegment es = new SetExtensionSegment(getUri(), property, object);
				es.setSubjects(subjects);
				m_les.saveExtensionSegment(es);
				break;
			case ExtensionManager.MODE_READONLY:
				throw new UnsupportedOperationException("read only mode: adding triples not permitted");
			}
		} catch (CorruptIndexException e) {
			throw new StorageException(e);
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}
}
