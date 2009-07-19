package edu.unika.aifb.graphindex.preprocessing;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.TripleSink;

public class DatasetAnalyzer implements TripleSink {

	private Map<String,Integer> m_properties;
	private static final Logger log = Logger.getLogger(DatasetAnalyzer.class);
	
	public DatasetAnalyzer() {
		m_properties = new HashMap<String,Integer>();
	}

	public void triple(String s, String p, String o, String objectType) {
		if (!m_properties.containsKey(p)) {
			m_properties.put(p, 1);
		}
		else
			m_properties.put(p, m_properties.get(p) + 1);
	}
	
	public void printAnalysis() {
		log.info("properties: " + m_properties.size());
		log.info("instance counts:");
		for (String p : m_properties.keySet())
			log.info("  " + p + ": " + m_properties.get(p));
	}
}
