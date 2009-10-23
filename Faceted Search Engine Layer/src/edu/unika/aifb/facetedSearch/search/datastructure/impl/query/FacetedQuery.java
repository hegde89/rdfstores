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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;

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
	private static final String QUERY = "Query";

	/*
	 * 
	 */
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(FacetedQuery.class);

	/*
	 * 
	 */
	private int m_varCount;
	private int m_queryCount;

	/*
	 * 
	 */
	private QueryGraph m_qGraph;

	/*
	 * 
	 */
	private StructuredQuery m_initialQuery;

	/*
	 * 
	 */
	private Map<String, String> m_oldVar2newVarMap;
	private List<Map<String, String>> m_facetFacetValue2QueryMapList;

	/*
	 * 
	 */
	private ArrayList<Map<? extends Object, ? extends Object>> m_maps;

	public FacetedQuery() {

		m_qGraph = new QueryGraph();
		init();
	}

	public FacetedQuery(StructuredQuery query) {

		m_qGraph = query.getQueryGraph();
		init();
	}

	public String addFacetFacetValueTupleStrg(String ffvTupleString) {

		Map<String, String> facetFacetValue2Query = new HashMap<String, String>();
		facetFacetValue2Query.put(ffvTupleString, getNextAbstractQuery());
		m_facetFacetValue2QueryMapList.add(facetFacetValue2Query);

		return facetFacetValue2Query.get(ffvTupleString);
	}

	public void clean() {

		for (Map<? extends Object, ? extends Object> map : m_maps) {
			map.clear();
		}

		m_facetFacetValue2QueryMapList.clear();
	}

	public void clearOldVar2newVarMap() {
		m_oldVar2newVarMap.clear();
	}

	public Iterator<Map<String, String>> getFacetFacetValueTuple2QueryIterator() {
		return m_facetFacetValue2QueryMapList.iterator();
	}

	public StructuredQuery getInitialQuery() {
		return m_initialQuery;
	}

	public String getNextAbstractQuery() {

		m_varCount++;
		String nextVar = VAR_Q + m_varCount;

		return nextVar;
	}

	public String getNextVar() {

		m_queryCount++;
		String nextQuery = QUERY + m_queryCount;

		return nextQuery;
	}

	public Map<String, String> getOldVar2newVarMap() {
		return m_oldVar2newVarMap;
	}

	public QueryGraph getQGraph() {
		return m_qGraph;
	}

	private void init() {

		/*
		 * 
		 */
		m_oldVar2newVarMap = new HashMap<String, String>();
		m_facetFacetValue2QueryMapList = new ArrayList<Map<String, String>>();

		/*
		 * 
		 */
		m_maps = new ArrayList<Map<? extends Object, ? extends Object>>();
		m_maps.add(m_oldVar2newVarMap);

		/*
		 * 
		 */
		m_varCount = 0;
		m_queryCount = 0;
	}

	public void mergeWithAdditionalQuery(String domain, StructuredQuery sq) {

		// QNode domainQNode;
		//
		// if (m_qGraph.vertexSet().size() == 0) {
		//
		// domainQNode = new QNode(domain);
		// m_qGraph.addVertex(domainQNode);
		//
		// } else if
		// (domain.equals(FacetEnvironment.DefaultValue.KEYWORD_DOMAIN)) {
		//
		// domainQNode = m_qGraph
		// .getNodeByLabel(FacetEnvironment.DefaultValue.VAR);
		// m_oldVar2newVarMap.put(domain, FacetEnvironment.DefaultValue.VAR);
		//
		// } else {
		//
		// domainQNode = m_qGraph.getNodeByLabel(domain);
		//
		// if (domainQNode == null) {
		//
		// Iterator<QNode> vertexIter = m_qGraph.vertexSet().iterator();
		//
		// while (vertexIter.hasNext()) {
		//
		// QNode nextNode = vertexIter.next();
		//
		// if (nextNode.getLabel().startsWith(
		// FacetEnvironment.DefaultValue.VAR_PREFIX)) {
		//
		// domainQNode = nextNode;
		// m_oldVar2newVarMap.put(domain, nextNode.getLabel());
		// break;
		// }
		// }
		// }
		// }
		//
		// QueryGraph queryGraph = sq.getQueryGraph();
		//
		// String startLabel;
		//
		// if (m_oldVar2newVarMap.containsKey(domain)) {
		// startLabel = domain;
		// } else {
		// startLabel = domainQNode.getLabel();
		// }
		//
		// QNode startNode = queryGraph.getNodeByLabel(startLabel);
		//
		// Stack<QueryEdge> todoStack = new Stack<QueryEdge>();
		// todoStack.addAll(queryGraph.outgoingEdgesOf(startNode));
		//
		// while (!todoStack.isEmpty()) {
		//
		// QueryEdge oldEdge = todoStack.pop();
		// String srcLabel = oldEdge.getSource().getLabel();
		// String tarLabel = oldEdge.getTarget().getLabel();
		//
		// if (m_oldVar2newVarMap.containsKey(srcLabel)) {
		// srcLabel = m_oldVar2newVarMap.get(srcLabel);
		// }
		//
		// if (FacetUtils.isVariable(tarLabel)) {
		//
		// m_qGraph.addEdge(srcLabel, oldEdge.getProperty(), getNextVar());
		// m_oldVar2newVarMap.put(tarLabel, getNextVar());
		//
		// } else {
		//
		// m_qGraph.addEdge(srcLabel, oldEdge.getProperty(), tarLabel);
		//
		// }
		//
		// todoStack.addAll(queryGraph.outgoingEdgesOf(oldEdge.getTarget()));
		// }
		//
		// m_oldVar2newVarMap.clear();
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

	public void setInitialQuery(StructuredQuery initialQuery) {
		m_initialQuery = initialQuery;
	}
}