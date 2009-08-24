package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.equivalence.EquivalenceComparator;
import org.jgrapht.experimental.isomorphism.GraphIsomorphismInspector;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.exploration.StructuredMatchElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.util.Util;

import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;;

public class Subgraph extends DefaultDirectedGraph<NodeElement,EdgeElement> implements Comparable<Subgraph> {
	private static final long serialVersionUID = -5730502189634789126L;
	
	private Set<Cursor> m_cursors;
	private Set<EdgeElement> m_edges;
	private int m_cost;
	private Map<String,String> m_label2var;
	private NodeElement m_structuredNode = null;
	private Set<Map<NodeElement,NodeElement>> m_mappings;
	private Set<NodeElement> m_augmentedNodes;
	private Map<NodeElement,Set<KeywordSegment>> m_nodeSegments;

	private HashMap<String,Set<KeywordSegment>> m_select2ks;


	private static final Logger log = Logger.getLogger(Subgraph.class);
	
	public Subgraph(Class<? extends EdgeElement> arg0) {
		super(arg0);
		m_mappings = new HashSet<Map<NodeElement,NodeElement>>();
		m_edges = new HashSet<EdgeElement>();
		m_label2var = new HashMap<String,String>();
		m_augmentedNodes = new HashSet<NodeElement>();
		m_nodeSegments = new HashMap<NodeElement,Set<KeywordSegment>>();
	}

	public Subgraph(Set<Cursor> cursors) {
		this(EdgeElement.class);
		
		m_cursors = cursors;
		for (Cursor c : cursors) {
			if (c.getCost() > m_cost)
				m_cost = c.getCost();
			
			if (c instanceof StructuredQueryCursor)
				m_structuredNode = (NodeElement)c.getStartCursor().getGraphElement();
			else {
				Cursor startCursor = c.getStartCursor();
				NodeElement startNode = (NodeElement)startCursor.getGraphElement();
				Set<KeywordSegment> kss = m_nodeSegments.get(startNode);
				if (kss == null) {
					kss = new HashSet<KeywordSegment>();
					m_nodeSegments.put(startNode, kss);
				}
				kss.addAll(startCursor.getKeywordSegments());
			}
			
			for (EdgeElement e : c.getEdges()) {
				m_edges.add((EdgeElement)e);
				
				addVertex(((EdgeElement)e).getSource());
				addVertex(((EdgeElement)e).getTarget());
				addEdge(((EdgeElement)e).getSource(), ((EdgeElement)e).getTarget(), (EdgeElement)e);
			}
		}

		NodeElement start = null;
		for (NodeElement node : vertexSet()) {
			if (inDegreeOf(node) + outDegreeOf(node) == 1) {
				start = node;
				break;
			}
		}	
		if (start != null)
			m_cost = getLongestPath(start, new ArrayList<String>());
		else
			m_cost = m_edges.size();
		
//		if (m_edges.size() == 3 && m_cost == 3)
//			log.debug("blah");
	}
	
	public Set<Cursor> getCursors() {
		return m_cursors;
	}
	
	public NodeElement getStructuredNode() {
		return m_structuredNode;
	}
	
	public void addMappings(List<Map<NodeElement,NodeElement>> maps) {
		m_mappings.addAll(maps);
	}
	
	public Set<Map<NodeElement,NodeElement>> getMappings() {
		return m_mappings;
	}
	
	private int getLongestPath(NodeElement node, List<String> path) {
		List<String> newPath = new ArrayList<String>(path);
		newPath.add(node.getLabel());
		
		Set<NodeElement> next = new HashSet<NodeElement>();
		
		for (EdgeElement edge : outgoingEdgesOf(node))
			if (!path.contains(edge.getTarget().getLabel()))
				next.add(edge.getTarget());
		for (EdgeElement edge : incomingEdgesOf(node)) 
			if (!path.contains(edge.getSource().getLabel()))
				next.add(edge.getSource());
		
		int max = 0;
		for (NodeElement nextNode : next) {
			int length = getLongestPath(nextNode, newPath);
			if (length > max)
				max = length;
		}
		
		return Math.max(max, newPath.size() - 1);
	}
	
	public int getCost() {
		return m_cost;
	}

	public int compareTo(Subgraph o) {
		return ((Integer)getCost()).compareTo(o.getCost());
	}
	
	private void addAugmentedEdges() {
		for (NodeElement node : m_nodeSegments.keySet()) {
			for (String property : node.getAugmentedEdges().keySet()) {
				m_augmentedNodes.add(node);
				
				Set<KeywordSegment> segments = m_nodeSegments.get(node);
//				log.debug("node: " + node + ", property: " + property + ", segments: " + segments);
				
				List<KeywordSegment> augmentedKS = node.getAugmentedEdges().get(property);
//				log.debug(" augmented ks: "+ augmentedKS);
				for (KeywordSegment ks : augmentedKS) {
					if (segments.contains(ks)) {
						NodeElement target = new NodeElement(ks.toString());
						addVertex(target);
						addEdge(node, target, new EdgeElement(node, property, target));
						
						m_label2var.put(target.getLabel(), target.getLabel());
					}
				}
			}
		}
	}
	
	private TranslatedQuery attachQuery(StructuredQuery query, QNode var, Table<String> indexMatches, List<Table<String>> resultTables) {
		TranslatedQuery q = new TranslatedQuery(query.getName() + "-" + var.getLabel(), var);
		
		Map<String,String> label2var = new HashMap<String,String>(m_label2var);
		label2var.put(m_structuredNode.getLabel(), var.getLabel());
		
		// index match table first contains column only for the explored subgraph,
		// those for the query will be joined later by the query evaluator
		indexMatches.setColumnName(indexMatches.columnCount() - 1, var.getLabel());
		// add connecting var to all ext mappings (is the last in each row)
//		for (String[] row : indexMatches)
//			row[row.length - 1] = m_structuredNode.getLabel();
		
		q.setIndexMatches(indexMatches);
		
		for (EdgeElement edge : edgeSet()) {
			if (Util.isVariable(edge.getTarget().getLabel()))
				q.addEdge(label2var.get(edge.getSource().getLabel()), edge.getLabel(), label2var.get(edge.getTarget().getLabel()));
			else
				q.addAttributeEdge(label2var.get(edge.getSource().getLabel()), edge.getLabel(), label2var.get(edge.getTarget().getLabel()));
		}
		for (QueryEdge edge : query.getQueryGraph().edgeSet()) 
			q.addStructuredEdge(edge.getSource(), edge.getLabel(), edge.getTarget());

		for (QNode node : query.getVariables())
			q.setAsSelect(node.getLabel());
		for (NodeElement node : m_augmentedNodes)
			q.setAsSelect(m_label2var.get(node.getLabel()));
		
		for (Table<String> table : resultTables) {
			Table<String> copy = new Table<String>(table, true);
			copy.setColumnName(0, label2var.get(copy.getColumnName(0)));
			q.addResult(copy);
		}
		
		return q;
	}
	
	public List<TranslatedQuery> attachQuery(StructuredQuery query, Map<String,Set<QNode>> ext2vars) {
		List<TranslatedQuery> queries = new ArrayList<TranslatedQuery>();
		
		addAugmentedEdges();

		int x = 0;
		for (EdgeElement edge : edgeSet()) {
			String src = m_label2var.get(edge.getSource().getLabel());
			if (src == null && !edge.getSource().equals(m_structuredNode)) {
				src = "?x" + ++x;
				m_label2var.put(edge.getSource().getLabel(), src);
			}
			
			String trg = m_label2var.get(edge.getTarget().getLabel());
			if (trg == null && !edge.getTarget().equals(m_structuredNode)) {
				trg = "?x" + ++x;
				m_label2var.put(edge.getTarget().getLabel(), trg);
			}
		}

		// m_mappings contains ext->ext mappings from this subgraph to
		// isomorphic subgraphs found during exploration
		// create maps containing query var->extension mappings
		List<String> columns = new ArrayList<String>();
		for (String ext : m_label2var.keySet()) {
			String v = m_label2var.get(ext);
			if (Util.isVariable(v))
				columns.add(v);
		}
		if (query != null) {
			columns.add("?PLACEHOLDER");
			m_label2var.put(m_structuredNode.getLabel(), "?PLACEHOLDER");
		}
		
		Table<String> indexMatches = new Table<String>(columns);
		
		// don't forget this subgraph
		String[] row = new String[indexMatches.columnCount()];
		for (String ext : m_label2var.keySet()) {
			String v = m_label2var.get(ext);
			if (Util.isVariable(v))
				row[indexMatches.getColumn(v)] = ext;
		}
		indexMatches.addRow(row);
		
		// from the isomorphic subgraphs
		for (Map<NodeElement,NodeElement> extMap : m_mappings) {
			row = new String[indexMatches.columnCount()];
			for (NodeElement node : extMap.keySet()) {
				String ext = node.getLabel();
				String v = m_label2var.get(ext);
				if (v != null && Util.isVariable(v)) // v is null if the node is the connecting node to a structured query
					row[indexMatches.getColumn(v)] = extMap.get(node).getLabel();
			}
			indexMatches.addRow(row);
		}
		
		// build a result table for each augmented edge
		List<Table<String>> resultTables = new ArrayList<Table<String>>();
		for (NodeElement augmentedNode : m_augmentedNodes) {
			Set<KeywordSegment> kss = m_nodeSegments.get(augmentedNode);
			assert kss.size() == 1;
			for (KeywordSegment ks : kss) {
				Table<String> table = new Table<String>(augmentedNode.getLabel(), ks.toString());
				table.addRows(augmentedNode.getSegmentEntities(ks).getRows());
				Set<String> alreadyAdded = new HashSet<String>();
				for (Map<NodeElement,NodeElement> map : m_mappings)
					if (alreadyAdded.add(map.get(augmentedNode).getLabel()))
						table.addRows(map.get(augmentedNode).getSegmentEntities(ks).getRows());
				resultTables.add(table);
			}
		}

		if (query != null) {
			// in the results of the structured query, a single variable
			// may be mapped to different extensions in different answers
			// if these extensions occur in the current subgraph as nodes, we can
			// attach the structured query at multiple points
			for (QNode var : ext2vars.get(m_structuredNode.getLabel())) {
				// deep copy extension mappings, which will be extended by attachQuery
				Table<String> copy = new Table<String>(indexMatches, false);
				for (String[] r : indexMatches)
					copy.addRow(r.clone());
				queries.add(attachQuery(query, var, copy, resultTables));
			}
		}
		else {
			TranslatedQuery q = new TranslatedQuery("qt", null);
			for (EdgeElement edge : edgeSet()) {
				if (Util.isVariable(m_label2var.get(edge.getTarget().getLabel())))
					q.addEdge(m_label2var.get(edge.getSource().getLabel()), edge.getLabel(), m_label2var.get(edge.getTarget().getLabel()));
				else
					q.addAttributeEdge(m_label2var.get(edge.getSource().getLabel()), edge.getLabel(), m_label2var.get(edge.getTarget().getLabel()));
			}

			q.setIndexMatches(indexMatches);
			
			for (Table<String> table : resultTables) {
				Table<String> copy = new Table<String>(table, true);
				copy.setColumnName(0, m_label2var.get(copy.getColumnName(0)));
				q.addResult(copy);
				q.setAsSelect(copy.getColumnName(0));
			}
			
			queries.add(q);
		}
		
		return queries;
	}
	
	@SuppressWarnings("unchecked")
	public List<Map<NodeElement,NodeElement>> isIsomorphicTo(Subgraph sg) {
		GraphIsomorphismInspector<IsomorphismRelation> t =  AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(this, sg, 
			new EquivalenceComparator() {
				public boolean equivalenceCompare(Object arg0, Object arg1, Object arg2, Object arg3) {
					return true;
				}

				public int equivalenceHashcode(Object arg0, Object arg1) {
					return 0;
				}
			}, 
			new EquivalenceComparator() {
				public boolean equivalenceCompare(Object arg0, Object arg1, Object arg2, Object arg3) {
					return ((EdgeElement)arg0).getLabel().equals(((EdgeElement)arg1).getLabel());
				}
	
				public int equivalenceHashcode(Object arg0, Object arg1) {
					return arg0.hashCode();
				}
			});

		List<Map<NodeElement,NodeElement>> mappings = new ArrayList<Map<NodeElement,NodeElement>>();
		
		if (sg.vertexSet().size() == vertexSet().size() && sg.edgeSet().size() == edgeSet().size() && t.isIsomorphic()) {
			while (t.hasNext()) {
				IsomorphismRelation rel = t.next();

				HashMap<NodeElement,NodeElement> mapping = new HashMap<NodeElement,NodeElement>();
				for (NodeElement v1 : vertexSet()) {
					mapping.put(v1, (NodeElement)rel.getVertexCorrespondence(v1, true));
				}
				mappings.add(mapping);
			}
		}
			
		return mappings;
	}
	
	public String toString() {
		return "subgraph size: " + m_edges.size() + ", cost: " + m_cost + ", strucstart: " + m_structuredNode + ", " + m_edges;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_edges == null) ? 0 : m_edges.hashCode());
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
		Subgraph other = (Subgraph)obj;
		if (m_edges == null) {
			if (other.m_edges != null)
				return false;
		} else if (!m_edges.equals(other.m_edges))
			return false;
		return true;
	}

	public boolean hasDanglingEdge() {
		if (edgeSet().size() == 1)
			return false;
		
		for (NodeElement node : vertexSet()) {
			// a node is the end of a dangling edge if
			// - if it's a "dead end", i.e. total degree is 1
			// - the node has no augmented edges (which will be attached later)
			// - the node is not the link to the structured query (if there is any)
			if (outDegreeOf(node) + inDegreeOf(node) == 1 && node.getAugmentedEdges().size() == 0 && !node.equals(m_structuredNode))
				return true;
		}
		
		return false;
	}
}
