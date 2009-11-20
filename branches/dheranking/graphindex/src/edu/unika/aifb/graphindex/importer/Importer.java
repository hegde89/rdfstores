package edu.unika.aifb.graphindex.importer;

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
import java.util.Collection;
import java.util.List;

import org.openrdf.model.vocabulary.XMLSchema;

public abstract class Importer {

	private String defaultDataType = XMLSchema.STRING.toString();
	private boolean m_ignoreDataTypes = true;

	protected TripleSink m_sink;
	protected List<String> m_files;

	protected Importer() {
		m_files = new ArrayList<String>();
	}

	public void addImport(String fileName) {
		m_files.add(fileName);
	}

	public void addImports(Collection<String> fileNames) {
		m_files.addAll(fileNames);
	}

	public abstract void doImport();

	/**
	 * @return the defaultDataType
	 */
	public String getDefaultDataType() {
		return defaultDataType;
	}

	/**
	 * @return the m_ignoreDataTypes
	 */
	public boolean ignoreDataTypesEnabled() {
		return m_ignoreDataTypes;
	}

	/**
	 * @param defaultDataType
	 *            the defaultDataType to set
	 */
	public void setDefaultDataType(String defaultDataType) {
		this.defaultDataType = defaultDataType;
	}

	/**
	 * @param m_ignoreDataTypes
	 *            the m_ignoreDataTypes to set
	 */
	public void setIgnoreDataTypes(boolean m_ignoreDataTypes) {
		this.m_ignoreDataTypes = m_ignoreDataTypes;
	}

	public void setTripleSink(TripleSink gb) {
		m_sink = gb;
	}
}
