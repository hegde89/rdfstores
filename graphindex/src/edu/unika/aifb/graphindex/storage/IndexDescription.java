package edu.unika.aifb.graphindex.storage;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * The IndexDescription is used to describe an index. An index consists of a list of index fields, which
 * describes the indexed term, and one value field. Each of these fields is a {@link DataField}, 
 * which identifies a part of the information to index. In this case, usually triples or quads are 
 * indexed and so the fields are subject, property, object, context, etc. 
 * 
 * An index is identified by its index and value field name. If several indexes are stored in the same 
 * storage location (e.g. a Lucene directory), the index and value field names have to be unique.
 * 
 * The storage layer supports term and prefix queries. A term query specifies values for all index fields,
 * whereas for prefix queries only a prefix is specified.
 * 
 * @author gla
 */
public class IndexDescription {
	/**
	 * The name of the index field. Used by the storage layer, needs to be unique for a single storage
	 * location.
	 */
	private String m_indexFieldName;
	/**
	 * The name of the value field. Used by the storage layer, needs to be unique for a single storage
	 * location.
	 */
	private String m_valueFieldName;
	/**
	 * List of fields that make up the index term. Order is significant.
	 */
	private List<DataField> m_indexFields;
	/**
	 * The field stored as values.
	 */
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
	
	public static final IndexDescription OES = new IndexDescription("oes", "es",
		DataField.OBJECT, DataField.EXT_SUBJECT);
	public static final IndexDescription SES = new IndexDescription("ses", "es",
		DataField.SUBJECT, DataField.EXT_SUBJECT);
	
	public static final IndexDescription EXTENT = new IndexDescription("ext", "ent",
		DataField.EXT, DataField.ENT);
	
	public static final IndexDescription OPS = new IndexDescription("op", "s",
		DataField.OBJECT, DataField.PROPERTY, DataField.SUBJECT);
	public static final IndexDescription SPO = new IndexDescription("sp", "o",
		DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT);
	public static final IndexDescription POS = new IndexDescription("po", "s",
		DataField.PROPERTY, DataField.OBJECT, DataField.SUBJECT);
	public static final IndexDescription PSO = new IndexDescription("ps", "o",
		DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT);
	public static final IndexDescription SOP = new IndexDescription("so", "p",
		DataField.SUBJECT, DataField.OBJECT, DataField.PROPERTY);

	public static final IndexDescription SCOP = new IndexDescription("sco", "p",
		DataField.SUBJECT, DataField.CONTEXT, DataField.OBJECT, DataField.PROPERTY);
	public static final IndexDescription OCPS = new IndexDescription("ocp", "s",
		DataField.OBJECT, DataField.CONTEXT, DataField.PROPERTY, DataField.SUBJECT);
	public static final IndexDescription PSOC = new IndexDescription("pso", "c",
		DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT, DataField.CONTEXT);
	public static final IndexDescription CPSO = new IndexDescription("cps", "o",
		DataField.CONTEXT, DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT);
	public static final IndexDescription POCS = new IndexDescription("poc", "s",
		DataField.PROPERTY, DataField.OBJECT, DataField.CONTEXT, DataField.SUBJECT);
	public static final IndexDescription SOPC = new IndexDescription("sop", "c",
		DataField.SUBJECT, DataField.OBJECT, DataField.PROPERTY, DataField.CONTEXT);
	
	// MappingIndex
	public static final IndexDescription DSEEDS = new IndexDescription("dseeds", "p",
			DataField.DS_SOURCE, DataField.E_SOURCE, DataField.E_TARGET, DataField.DS_TARGET, DataField.CONTEXT);


	// Facet indices
	
	/**
	 * Extension + subject (contained in this extension) > position in vector (for subject)
	 */
	public static final IndexDescription ESV = new IndexDescription("es", "v",
			DataField.EXT_SUBJECT, DataField.VECTOR_POS);
	
	/**
	 * Extension + literal1 + literal2 (both contained in this extension) > distance between lit1 and lit2
	 */
	public static final IndexDescription ELLD = new IndexDescription("ell", "d",
			DataField.EXT_SUBJECT, DataField.VECTOR_POS);
	
	
	/**
	 * Creates a new IndexDescription object. The parameter <code>fields</code> contains
	 * the index and value fields. The last element of the array is assumed to be the value field.
	 * All previous elements are indexed fields.
	 * 
	 * @param indexFieldName
	 * @param valueFieldName
	 * @param fields 
	 */
	public IndexDescription(String indexFieldName, String valueFieldName, DataField... fields) {
		m_indexFieldName = indexFieldName;
		m_valueFieldName = valueFieldName;
		m_indexFields = new ArrayList<DataField>();
		for (int i = 0; i < fields.length - 1; i++)
			m_indexFields.add(fields[i]);
		m_valueField = fields[fields.length - 1];
	}
	
	/**
	 * Convenience method for loading index description from a configuration file.
	 * 
	 * @param desc
	 */
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
	
	/**
	 * Returns the position of {@link DataField} <code>df</code> among the indexed
	 * fields or <code>-1</code> if the field is not part of the indexed fields.
	 * 
	 * @param df
	 * @return position of <code>df</code> among the indexed fields or <code>-1</code> if not
	 * present
	 */
	public int getIndexFieldPos(DataField df) {
		for (int i = 0; i < m_indexFields.size(); i++)
			if (m_indexFields.get(i) == df)
				return i;
		return -1;
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

	/**
	 * Checks if the index is compatible with the specified fields. An index is
	 * compatible if the index contains exactly the fields specified in the <code>fields</code>
	 * parameter. Again, the last element is assumed to be the value field.
	 * 
	 * @param fields
	 * @return true, if the index is compatible, false otherwise
	 */
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
	
	/**
	 * Creates an array containing the values specified by <code>indexValue</code> in the
	 * order of this index's indexed fields, so that the array can be used to access the index.
	 * 
	 * @param indexValues maps {@link DataField}s to their values
	 * @return values from <code>indexValues</code> in correct order for access to this index
	 */
	public String[] createValueArray(Map<DataField,String> indexValues) {
		String[] values = new String [indexValues.size()];
		
		for (int i = 0; i < values.length; i++) {
			values[i] = indexValues.get(m_indexFields.get(i)); 
			if (values[i] == null)
				return null;
		}
		
		return values;
	}
	
	/**
	 * Convenience method that accepts an array as a representation of a Map. See {@link #createValueArray(Map)}.
	 * The array should contain alternating DataField and String objects as keys and values, respectively.
	 *  
	 * @param map
	 * @return
	 */
	public String[] createValueArray(Object... map) {
		Map<DataField,String> values = new HashMap<DataField,String>();
		for (int i = 0; i < map.length; i += 2)
			values.put((DataField)map[i], (String)map[i + 1]);
		return createValueArray(values);
	}
	
	public int[] getIndexFieldMap(DataField... fields) {
		int[] map = new int [fields.length];
		
		for (int i = 0; i < fields.length; i++) {
			DataField indexField = m_indexFields.get(i);
			
			int idx = -1;
			for (int j = 0; j < fields.length; j++) {
				if (fields[j] == indexField) {
					idx = j;
					break;
				}
			}
			
			if (idx < 0)
				return null;
			
			map[idx] = i;
		}
		
		return map;
	}
	
	public boolean isPrefix(DataField[] fields) {
		return isPrefix(Arrays.asList(fields));
	}
	
	public boolean isPrefix(List<DataField> fields) {
		if (fields.size() > m_indexFields.size())
			return false;

		int fieldsFound = 0;
		
		for (DataField indexField : m_indexFields) {
			boolean found = false;
			for (DataField field : fields) {
				if (field == indexField) {
					found = true;
					break;
				}
			}
			
			if (found) {
				fieldsFound++;
			
				if (fieldsFound == fields.size())
					return true;
			}
			else
				return false;
		}
		
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_indexFields == null) ? 0 : m_indexFields.hashCode());
		result = prime * result + ((m_valueField == null) ? 0 : m_valueField.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexDescription other = (IndexDescription)obj;
		if (m_indexFields == null) {
			if (other.m_indexFields != null)
				return false;
		} else if (!m_indexFields.equals(other.m_indexFields))
			return false;
		if (m_valueField == null) {
			if (other.m_valueField != null)
				return false;
		} else if (!m_valueField.equals(other.m_valueField))
			return false;
		return true;
	}
	
	public String toString() {
		return "idx: " + m_indexFields + ", val: " + m_valueField;
	}
}