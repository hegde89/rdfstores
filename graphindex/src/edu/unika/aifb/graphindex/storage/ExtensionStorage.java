package edu.unika.aifb.graphindex.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Triple;


public interface ExtensionStorage {
	public class DataField {
		public static final String FIELD_SUBJECT = "s";
		public static final String FIELD_OBJECT = "o";
		public static final String FIELD_PROPERTY = "p";
		public static final String FIELD_EXT_SUBJECT = "es";
		public static final String FIELD_EXT_OBJECT = "eo";
		
		public static final DataField SUBJECT = new DataField(FIELD_SUBJECT);
		public static final DataField OBJECT = new DataField(FIELD_OBJECT);
		public static final DataField PROPERTY = new DataField(FIELD_PROPERTY);
		public static final DataField EXT_SUBJECT = new DataField(FIELD_EXT_SUBJECT);
		public static final DataField EXT_OBJECT = new DataField(FIELD_EXT_OBJECT);
		
		private String m_type;
		
		private DataField(String type) {
			m_type = type;
		}
		
		public static DataField getField(String type) {
			if (type.equals(FIELD_SUBJECT))
				return SUBJECT;
			if (type.equals(FIELD_OBJECT))
				return OBJECT;
			if (type.equals(FIELD_PROPERTY))
				return PROPERTY;
			if (type.equals(FIELD_EXT_SUBJECT))
				return EXT_SUBJECT;
			if (type.equals(FIELD_EXT_OBJECT))
				return EXT_OBJECT;
			return null;
		}
		
		public String toString() {
			return m_type;
		}
	}
	
	public class IndexDescription {
		private String m_indexFieldName, m_valueFieldName;
		private List<DataField> m_indexFields;
		private DataField m_valueField;
		
		public static final IndexDescription PSESO = new IndexDescription("pseso", "o", 
			DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT, DataField.OBJECT);
		public static final IndexDescription POESS = new IndexDescription("poess", "s", 
			DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT, DataField.SUBJECT);
		public static final IndexDescription PSEOO = new IndexDescription("pseoo", "o", 
			DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_OBJECT, DataField.OBJECT);
		public static final IndexDescription POEOS = new IndexDescription("poeos", "s", 
			DataField.PROPERTY, DataField.OBJECT, DataField.EXT_OBJECT, DataField.SUBJECT);
		
		public static final IndexDescription POES = new IndexDescription("poes", "es",
			DataField.PROPERTY, DataField.OBJECT, DataField.EXT_SUBJECT);
		public static final IndexDescription PSES = new IndexDescription("pses", "es",
			DataField.PROPERTY, DataField.SUBJECT, DataField.EXT_SUBJECT);
		
		public IndexDescription(String indexFieldName, String valueFieldName, DataField... fields) {
			m_indexFieldName = indexFieldName;
			m_valueFieldName = valueFieldName;
			m_indexFields = new ArrayList<DataField>();
			for (int i = 0; i < fields.length - 1; i++)
				m_indexFields.add(fields[i]);
			m_valueField = fields[fields.length - 1];
		}
		
		public IndexDescription(Map<String,String> desc) {
			m_indexFieldName = desc.get("index_field_name");
			m_valueFieldName = desc.get("value_field_name");
			m_valueField = DataField.getField(desc.get("value_field"));
			String[] t = desc.get("index_field").split(" ");
			m_indexFields = new ArrayList<DataField>();
			for (String field : t)
				m_indexFields.add(DataField.getField(field));
		}
		
		public String getIndexFieldName() {
			return m_indexFieldName;
		}
		
		public String getValueFieldName() {
			return m_valueFieldName;
		}
		
		public List<DataField> getIndexFields() {
			return m_indexFields;
		}
		
		public DataField getValueField() {
			return m_valueField;
		}
		
		public Map<String,String> asMap() {
			Map<String,String> map = new HashMap<String,String>();
			map.put("index_field_name", m_indexFieldName);
			map.put("value_field_name", m_valueFieldName);
			map.put("value_field", m_valueField.toString());
			String indexField = "";
			for (DataField f : m_indexFields)
				indexField += f.toString() + " ";
			map.put("index_field", indexField.trim());
			return map;
		}

		public boolean isCompatible(DataField[] fields) {
			if (fields.length - 1 != m_indexFields.size())
				return false;
			
			for (int i = 0; i < fields.length - 1; i++)
				if (fields[i] != m_indexFields.get(i))
					return false;
			
			if (fields[fields.length - 1] != m_valueField)
				return false;
				
			return true;
		}
	}
	
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
	
	public void addTriples(IndexDescription index, String ext, String property, String so, List<String> values) throws StorageException;
	
	public boolean hasTriples(String ext, String propertyUri, String object) throws StorageException;
	public boolean hasTriples(IndexDescription index, String ext, String property, String so) throws StorageException;
	public GTable<String> getIndexTable(IndexDescription index, String ext, String property, String so) throws StorageException;
	public List<GTable<String>> getIndexTables(IndexDescription index, String ext, String property) throws StorageException;
	public Set<String> getExtensions(IndexDescription index, String so) throws StorageException;
	public String getExtension(String object) throws StorageException;
	public boolean isValidObjectExtension(String object, String ext) throws StorageException;

	public void mergeExtensions() throws IOException, StorageException;
	public void clearCaches() throws StorageException;
	
	public void updateCacheSizes();
	public void createSEOE(Map<String,Set<String>> se, Set<String> oe) throws StorageException;
	public String concat(String[] values, int length);
	public void addData(IndexDescription index, String indexKey, List<String> set, boolean sort) throws StorageException;
	public void addData(IndexDescription index, String indexKey, Collection<String> set) throws StorageException;
	public List<String> getData(IndexDescription index, String... indexFields) throws StorageException;
	public Set<String> getDataSet(IndexDescription index, String... indexFields) throws StorageException;
}
