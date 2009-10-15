package edu.unika.aifb.graphindex.query;

import org.apache.log4j.Logger;

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

public class HybridQuery extends Query {
	private StructuredQuery m_structuredQuery;
	private KeywordQuery m_keywordQuery;
	private QNode m_attachNode;
	
	private static final Logger log = Logger.getLogger(HybridQuery.class);
	
	public HybridQuery(String name, StructuredQuery sq, KeywordQuery kq) {
		super(name);
		m_structuredQuery = sq;
		m_keywordQuery = kq;
	}

	public HybridQuery(String name, StructuredQuery sq, KeywordQuery kq, String attachNode) {
		super(name);
		m_structuredQuery = sq;
		m_keywordQuery = kq;
		m_attachNode = m_structuredQuery.getNode(attachNode);
		if (attachNode != null && m_attachNode == null) {
			log.warn("attach node " + attachNode + " not in query graph!");
			throw new IllegalArgumentException("attach node " + attachNode + " not in query graph!");
		}
	}
	
	public QNode getAttachNode() {
		return m_attachNode;
	}

	public StructuredQuery getStructuredQuery() {
		return m_structuredQuery;
	}
	
	public KeywordQuery getKeywordQuery() {
		return m_keywordQuery;
	}
}
