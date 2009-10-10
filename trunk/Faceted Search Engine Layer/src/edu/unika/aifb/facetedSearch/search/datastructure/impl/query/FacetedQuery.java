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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.facets.model.IRefinementPath;
import edu.unika.aifb.facetedSearch.facets.model.impl.FacetFacetValueRefinementPath;
import edu.unika.aifb.facetedSearch.facets.model.impl.QueryRefinementPath;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class FacetedQuery {

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
	private StructuredQuery m_query;

	/*
	 * 
	 */
	private Map<QueryEdge, Double> m_qEdge2genericNodeMap;

	/*
	 * 
	 */
	private Map<String, String> m_oldVar2newVarMap;

	/*
	 * 
	 */
	private List<QueryEdge> m_edge2genericNodes;

	public FacetedQuery() {

		StructuredQuery query = new StructuredQuery("");
		query.getQueryGraph().addVertex(new QNode(VAR_Q));

		m_query = query;
		m_qGraph = m_query.getQueryGraph();

		init();
	}

	public FacetedQuery(StructuredQuery query) {

		m_query = query;
		m_qGraph = query.getQueryGraph();

		init();
	}

	public void addPath(String domain, IRefinementPath path) {

		m_edge2genericNodes.clear();
		QNode domainQNode = m_qGraph.getNodeByLabel(domain);

		if (path instanceof FacetFacetValueRefinementPath) {

			FacetFacetValueRefinementPath ffvPath = (FacetFacetValueRefinementPath) path;
			QueryGraph queryGraph = ffvPath.getStructuredQuery()
					.getQueryGraph();

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

					m_qGraph.addEdge(srcLabel, oldEdge.getProperty(),
							getNextVar());

					m_oldVar2newVarMap.put(tarLabel, getNextVar());

				} else {

					QueryEdge newEdge = m_qGraph.addEdge(srcLabel, oldEdge
							.getProperty(), tarLabel);

					if (oldEdge.getTarget().getGenericNodeID() != -1) {

						m_edge2genericNodes.add(oldEdge);
						m_qEdge2genericNodeMap.put(newEdge, oldEdge.getTarget()
								.getGenericNodeID());
					}
				}

				todoStack.addAll(queryGraph
						.outgoingEdgesOf(oldEdge.getTarget()));
			}
		} else {

			StructuredQuery sQuery = ((QueryRefinementPath) path)
					.getStructuredQuery();
			QueryGraph queryGraph = sQuery.getQueryGraph();

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

					m_qGraph.addEdge(srcLabel, oldEdge.getProperty(),
							getNextVar());

					m_oldVar2newVarMap.put(tarLabel, getNextVar());

				} else {

					m_qGraph.addEdge(srcLabel, oldEdge.getProperty(), tarLabel);

				}

				todoStack.addAll(queryGraph
						.outgoingEdgesOf(oldEdge.getTarget()));
			}
		}
	}

	public void clearOldVar2newVarMap() {
		m_oldVar2newVarMap.clear();
	}

	public List<QueryEdge> getEdges2GenericNodes() {
		return m_edge2genericNodes;
	}

	private String getNextVar() {

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

	public StructuredQuery getQuery() {
		return m_query;
	}

	public boolean hasGenericNodes() {
		return !m_qEdge2genericNodeMap.isEmpty();
	}

	private void init() {

		m_qEdge2genericNodeMap = new HashMap<QueryEdge, Double>();
		m_oldVar2newVarMap = new HashMap<String, String>();
		m_edge2genericNodes = new ArrayList<QueryEdge>();
		m_varCount = 0;
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

			if (m_qEdge2genericNodeMap.containsKey(nextEdge)) {
				m_qEdge2genericNodeMap.remove(nextEdge);
			}
		}

		return success;
	}
}
