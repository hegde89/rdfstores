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
	
	public static final IndexDescription OES = new IndexDescription("oes", "es",
		DataField.OBJECT, DataField.EXT_SUBJECT);
	public static final IndexDescription SES = new IndexDescription("ses", "es",
		DataField.SUBJECT, DataField.EXT_SUBJECT);
	
	public static final IndexDescription OP = new IndexDescription("op", "s",
		DataField.OBJECT, DataField.PROPERTY, DataField.SUBJECT);
	public static final IndexDescription SP = new IndexDescription("sp", "o",
		DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT);
	public static final IndexDescription PO = new IndexDescription("po", "s",
		DataField.PROPERTY, DataField.OBJECT, DataField.SUBJECT);
	public static final IndexDescription PS = new IndexDescription("ps", "o",
		DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT);
	

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