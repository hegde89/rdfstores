/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.search.datastructure.impl.query;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class FacetedQuery implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4412170844553508893L;

	/*
	 * 
	 */
	private static final String VAR_Q = "?q";

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(FacetedQuery.class);

	/*
	 * 
	 */
	private int m_varCount;

	/*
	 * 
	 */
	private QueryGraph m_qGraph;

	/*
	 * 
	 */
	private Map<String, String> m_oldVar2newVarMap;

	public FacetedQuery() {

		m_qGraph = new QueryGraph();
		m_qGraph.addVertex(new QNode(VAR_Q));
		init();
	}

	public FacetedQuery(StructuredQuery query) {

		m_qGraph = query.getQueryGraph();
		init();
	}

	public void clearOldVar2newVarMap() {
		m_oldVar2newVarMap.clear();
	}

	public String getNextVar() {

		String nextVar = VAR_Q + m_varCount;
		m_varCount++;

		return nextVar;
	}

	public Map<String, String> getOldVar2newVarMap() {
		return m_oldVar2newVarMap;
	}

	public QueryGraph getQGraph() {
		return m_qGraph;
	}

	private void init() {

		m_oldVar2newVarMap = new HashMap<String, String>();
		m_varCount = 0;
	}

	public void mergeWithAdditionalQuery(String domain, StructuredQuery sq) {

		QNode domainQNode = m_qGraph.getNodeByLabel(domain);
		QueryGraph queryGraph = sq.getQueryGraph();
		QNode startNode = queryGraph.getNodeByLabel(domainQNode.getLabel());

		Stack<QueryEdge> todoStack = new Stack<QueryEdge>();
		todoStack.addAll(queryGraph.outgoingEdgesOf(startNode));

		while (!todoStack.isEmpty()) {

			QueryEdge oldEdge = todoStack.pop();
			String srcLabel = oldEdge.getSource().getLabel();
			String tarLabel = oldEdge.getTarget().getLabel();

			if (m_oldVar2newVarMap.containsKey(srcLabel)) {
				srcLabel = m_oldVar2newVarMap.get(srcLabel);
			}

			if (Util.isVariable(tarLabel)) {

				m_qGraph.addEdge(srcLabel, oldEdge.getProperty(), getNextVar());
				m_oldVar2newVarMap.put(tarLabel, getNextVar());

			} else {

				m_qGraph.addEdge(srcLabel, oldEdge.getProperty(), tarLabel);

			}

			todoStack.addAll(queryGraph.outgoingEdgesOf(oldEdge.getTarget()));
		}
	}

	public boolean removePath(QNode node) {

		boolean success = true;

		Iterator<QueryEdge> inEdgesIter = m_qGraph.incomingEdgesOf(node)
				.iterator();

		while (inEdgesIter.hasNext()) {
			QueryEdge inEdge = inEdgesIter.next();
			success = success && m_qGraph.removeEdge(inEdge);
		}

		Stack<QueryEdge> todoStack = new Stack<QueryEdge>();
		todoStack.addAll(m_qGraph.outgoingEdgesOf(node));

		while (!todoStack.isEmpty()) {

			QueryEdge nextEdge = todoStack.pop();
			m_qGraph.removeVertex(nextEdge.getSource());

			if (m_qGraph.outDegreeOf(nextEdge.getTarget()) > 0) {

				todoStack
						.addAll(m_qGraph.outgoingEdgesOf(nextEdge.getTarget()));
			} else {

				success = success
						&& m_qGraph.removeVertex(nextEdge.getTarget());
			}

		}

		return success;
	}
}
