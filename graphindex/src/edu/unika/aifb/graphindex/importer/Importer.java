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

import org.apache.log4j.Logger;

public abstract class Importer {

	protected TripleSink m_sink;
	protected List<String> m_files;

	protected static Logger log;
	
	protected Importer() {
		m_files = new ArrayList<String>();
	}
	
	public void addImport(String fileName) {
		m_files.add(fileName);
	}
	
	public void addImports(Collection<String> fileNames) {
		m_files.addAll(fileNames);
	}

	public void setTripleSink(TripleSink gb) {
		m_sink = gb;
	}

	public abstract void doImport();
}
