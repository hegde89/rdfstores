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

public class DataField {
	public static final String FIELD_SUBJECT = "s";
	public static final String FIELD_OBJECT = "o";
	public static final String FIELD_PROPERTY = "p";
	public static final String FIELD_EXT_SUBJECT = "es";
	public static final String FIELD_EXT_OBJECT = "eo";
	public static final String FIELD_CONTEXT = "c";
	public static final String FIELD_EXT = "ext";
	public static final String FIELD_ENT = "ent";
	
	public static final DataField SUBJECT = new DataField(FIELD_SUBJECT);
	public static final DataField OBJECT = new DataField(FIELD_OBJECT);
	public static final DataField PROPERTY = new DataField(FIELD_PROPERTY);
	public static final DataField EXT_SUBJECT = new DataField(FIELD_EXT_SUBJECT);
	public static final DataField EXT_OBJECT = new DataField(FIELD_EXT_OBJECT);
	public static final DataField CONTEXT = new DataField(FIELD_CONTEXT);
	
	public static final DataField EXT = new DataField(FIELD_EXT);
	public static final DataField ENT = new DataField(FIELD_ENT);
	
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
		if (type.equals(FIELD_CONTEXT))
			return CONTEXT;
		return null;
	}
	
	public String toString() {
		return m_type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_type == null) ? 0 : m_type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		return false;
	}
}