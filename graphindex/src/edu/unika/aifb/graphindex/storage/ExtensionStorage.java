package edu.unika.aifb.graphindex.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Triple;


public interface ExtensionStorage {
	public static final class Index {
		public static final String FIELD_S = "s";
		public static final String FIELD_O = "o";
		public static final String FIELD_E = "e";
		
		public static final Index EPO = new Index("epo", FIELD_S);
		public static final Index EPS = new Index("eps", FIELD_O);
		public static final Index OE = new Index("oe", FIELD_E);
		public static final Index SE = new Index("se", FIELD_E);
		
		private String m_indexField;
		private String m_valField;
		
		private Index(String indexField, String valField) { 
			m_indexField = indexField;
			m_valField = valField;
		}
		
		public String getIndexField() {
			return m_indexField;
		}
		
		public String getValField() {
			return m_valField;
		}
		
		public String toString() {
			return m_indexField;
		}
	}

	public void initialize(boolean clean, boolean readonly) throws StorageException;
	public void close() throws StorageException;
	
	public void startBulkUpdate() throws StorageException;
	public void finishBulkUpdate() throws StorageException;
	
	public Set<String> loadExtensionList() throws StorageException;
	public void saveExtensionList(Set<String> uris) throws StorageException;
	public void setExtensionManager(ExtensionManager extensionManager);
	public void optimize() throws StorageException;
	
	public void addTriples(Index index, String ext, String property, String so, List<String> values) throws StorageException;
	
	public boolean hasTriples(String ext, String propertyUri, String object) throws StorageException;
	public boolean hasTriples(Index index, String ext, String property, String so) throws StorageException;
//	public List<Triple> getTriples(String extUri, String property, String object) throws StorageException;
//	public List<Triple> getTriples(String extUri, String property) throws StorageException;
//	public GTable<String> getTable(String extUri, String property, String object, String allowedSubject) throws StorageException;
	public GTable<String> getIndexTable(Index index, String ext, String property, String so) throws StorageException;
	public List<GTable<String>> getIndexTables(Index index, String ext, String property) throws StorageException;
	public Set<String> getExtensions(Index index, String so) throws StorageException;
	public String getExtension(String object) throws StorageException;
	public boolean isValidObjectExtension(String object, String ext) throws StorageException;

	public void mergeExtensions() throws IOException, StorageException;
	public void clearCaches() throws StorageException;
	
	public void updateCacheSizes();
	public void createSEOE(Map<String,Set<String>> se, Set<String> oe) throws StorageException;
}
