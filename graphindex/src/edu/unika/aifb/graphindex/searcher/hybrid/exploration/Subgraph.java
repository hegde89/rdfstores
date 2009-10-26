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
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordQNode;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.util.Statistics;
import edu.unika.aifb.graphindex.util.Util;

import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;;

public class Subgraph extends DirectedMultigraph<NodeElement,EdgeElement> implements Comparable<Subgraph> {
	private static final long serialVersionUID = -5730502189634789126L;
	
	private Set<Cursor> m_cursors;
	private Set<EdgeElement> m_edges;
	private double m_cost;
	private Map<String,String> m_label2var;
	private NodeElement m_structuredNode = null;
	private Set<Map<NodeElement,NodeElement>> m_mappings;
	private Set<NodeElement> m_augmentedNodes;
	private Map<NodeElement,Set<KeywordSegment>> m_nodeSegments;

	private HashMap<String,Set<KeywordSegment>> m_select2ks;

	private Set<String> structuredEntities;
	
	private boolean m_valid = true;
	private Map<String,String> m_rename;
	private Map<String,KeywordQNode> m_keywordNodes;


	private static final Logger log = Logger.getLogger(Subgraph.class);

	public boolean track = false;
	
	public Subgraph(Class<? extends EdgeElement> arg0) {
		super(arg0);
		m_mappings = new HashSet<Map<NodeElement,NodeElement>>();
		m_edges = new HashSet<EdgeElement>();
		m_label2var = new HashMap<String,String>();
		m_augmentedNodes = new HashSet<NodeElement>();
		m_nodeSegments = new HashMap<NodeElement,Set<KeywordSegment>>();
		m_rename = new HashMap<String,String>();
		m_keywordNodes = new HashMap<String,KeywordQNode>();
	}

	public Subgraph(Set<Cursor> cursors) {
		this(EdgeElement.class);
		Map<String,Set<EdgeElement>> attributeEdges = new HashMap<String,Set<EdgeElement>>();
		
		m_cursors = cursors;
		
		Map<NodeElement,Set<String>> node2AllowedOutEdgeLabels = new HashMap<NodeElement,Set<String>>();
		Map<NodeElement,Set<String>> node2AllowedInEdgeLabels = new HashMap<NodeElement,Set<String>>();

		Set<NodeElement> keywordElementNodes = new HashSet<NodeElement>();
		
		Map<String,Set<String>> keywordEntities = new HashMap<String,Set<String>>();

		Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_CREATED);
		
		Statistics.start(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS);
		for (Cursor c : cursors) {
//			if (c.track) {
//				log.debug(c);
//				track = true;
//			}
			if (c.getStartCursor() instanceof StructuredQueryCursor) {
				m_structuredNode = (NodeElement)c.getStartCursor().getGraphElement();
				for (EdgeElement edge : c.getStartCursor().getEdges()) {
					m_label2var.put(edge.getSource().getLabel(), edge.getSource().getLabel());
					m_label2var.put(edge.getTarget().getLabel(), edge.getTarget().getLabel());
				}
				structuredEntities = ((StructuredQueryCursor)c.getStartCursor()).entities;
				m_label2var.remove(c.getStartCursor().getGraphElement().getLabel());
			}
			else {
				Cursor startCursor = c.getStartCursor();
				
				String name = m_rename.get(startCursor.getGraphElement().getLabel());
				if (name == null)
					name = startCursor.getKeywordSegments().toString();
				else {
					name += "," + startCursor.getKeywordSegments().toString();
					m_valid = false;
					Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_INVALID1);
					Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS);
					return;
				}
				
				m_rename.put(startCursor.getGraphElement().getLabel(), name);
				
				if (!m_keywordNodes.containsKey(name)) {
					KeywordQNode qnode = new KeywordQNode(name);
					for (KeywordSegment ks : startCursor.getKeywordSegments())
						for (String keyword : ks.getKeywords())
							qnode.addKeyword(keyword);
					m_keywordNodes.put(name, qnode);
				}
			}
			
			// find the first edge in the cursor chain
			Cursor cur = c.getParent();
			EdgeElement last = null;
			while (cur != null) {
				last = (EdgeElement)cur.getGraphElement();
				cur = cur.getParent().getParent();
			}
			
//			if (last != null) {
//				// there should be only one edge for each keyword matching element to eliminate stuff like (x->a, x->b)
//				Set<EdgeElement> edges = attributeEdges.get(last.getSource().getLabel());
//				if (edges == null) {
//					edges = new HashSet<EdgeElement>();
//					attributeEdges.put(last.getSource().getLabel(), edges);
//				}
//				edges.add(last);
//				
//				if (edges.size() > 1) {
//					m_valid = false;
//					Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_INVALID2);
//					Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS);
//					return;
//				}
//			}
			
			// find the second node cursor from the beginning (which is the keyword element cursor)
			cur = c;
			NodeCursor elementCursor = null;
			while (cur != null && cur.getParent() != null) {
				if (cur instanceof NodeCursor)
					elementCursor = (NodeCursor)cur;
				else {
					elementCursor = null;
					break;
				}
				cur = cur.getParent().getParent();
			}
			
			if (c.getStartCursor() instanceof StructuredQueryCursor)
				elementCursor = (NodeCursor)c.getStartCursor();
			
			
			if (elementCursor != null) {
				keywordElementNodes.add((NodeElement)elementCursor.getGraphElement());
				if (elementCursor instanceof KeywordNodeCursor)
					keywordEntities.put(elementCursor.getGraphElement().getLabel(), ((KeywordNodeCursor)elementCursor).m_keywordElement.entities);
				else if (elementCursor instanceof StructuredQueryCursor)
					keywordEntities.put(elementCursor.getGraphElement().getLabel(), ((StructuredQueryCursor)elementCursor).entities);

			}
			
			// retrieve allowed incoming edge labels
			if (elementCursor != null && elementCursor.getInProperties().size() > 0) {
				Set<String> allowed = node2AllowedInEdgeLabels.get((NodeElement)elementCursor.getGraphElement());
				if (allowed == null) {
					allowed = new HashSet<String>(elementCursor.getInProperties());
					node2AllowedInEdgeLabels.put((NodeElement)elementCursor.getGraphElement(), allowed);
				}
				allowed.addAll(elementCursor.getInProperties());
			}

			// retrieve allowed outgoing edge labels
			if (elementCursor != null && elementCursor.getOutProperties().size() > 0) {
				Set<String> allowed = node2AllowedOutEdgeLabels.get((NodeElement)elementCursor.getGraphElement());
				if (allowed == null) {
					allowed = new HashSet<String>(elementCursor.getOutProperties());
					node2AllowedOutEdgeLabels.put((NodeElement)elementCursor.getGraphElement(), allowed);
				}
				allowed.addAll(elementCursor.getOutProperties());
			}

			Statistics.start(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS_GRAPH);
			for (EdgeElement e : c.getEdges()) {
				m_edges.add(e);

//				if (e.getLabel().contains("writer"))
//					track = false;
				
				addVertex(((EdgeElement)e).getSource());
				addVertex(((EdgeElement)e).getTarget());
				addEdge(((EdgeElement)e).getSource(), ((EdgeElement)e).getTarget(), (EdgeElement)e);
			}
			Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS_GRAPH);
		}
		Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_CURSORS);

//		if (m_structuredNode != null) {
//			// subgraph is invalid if an entity edge and the structured part attach to the same node
//			for (NodeElement node : keywordElementNodes)
//				if (node.equals(m_structuredNode)) {
//					m_valid = false;
//					return;
//				}
//		}
		
		Set<String> values = new HashSet<String>(m_rename.values().size() + 5);
		values.addAll(m_rename.values());
		if (values.size() < m_rename.size()) {
			m_valid = false;
			Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_INVALID3);
			return;
		}
		
//		if (track)
//			log.debug(this);
		
		if (m_valid) {
			Statistics.start(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_ALLOWED_CHECK);
			for (NodeElement node : node2AllowedInEdgeLabels.keySet()) {
				Set<String> allowedEdgeLabels = node2AllowedInEdgeLabels.get(node);
				for (EdgeElement edge : edgesOf(node)) {
					if (edge.getTarget().equals(node) && !allowedEdgeLabels.contains(edge.getLabel()) && !m_rename.containsKey(edge.getTarget().getLabel())) {
						m_valid = false;
						Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_ALLOWED_CHECK);
						Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_INVALID4);
						
//						if (track) {
//							log.debug(edgeSet());
//							log.debug(" failed " + node + " in " + edge.getLabel() + ", allowed: " + allowedEdgeLabels);
//							log.debug(" " + keywordEntities);
//						}
						
						return;
					}
				}
			}
			
			for (NodeElement node : node2AllowedOutEdgeLabels.keySet()) {
				Set<String> allowedEdgeLabels = node2AllowedOutEdgeLabels.get(node);
				for (EdgeElement edge : edgesOf(node)) {
					if (edge.getSource().equals(node) && !allowedEdgeLabels.contains(edge.getLabel()) && !m_rename.containsKey(edge.getTarget().getLabel())) {
						m_valid = false;
						Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_ALLOWED_CHECK);
						Statistics.inc(Subgraph.class, Statistics.Counter.EX_SUBGRAPH_INVALID4);

//						if (track) {
//							log.debug(edgeSet());
//							log.debug(" failed " + node + " out " + edge.getLabel() + ", allowed: " + allowedEdgeLabels);
//							log.debug(" " + keywordEntities);
//						}
						
						return;
					}
				}
			}
			Statistics.end(Subgraph.class, Statistics.Timing.EX_SUBGRAPH_ALLOWED_CHECK);
		}

		m_cost = 0;
		for (EdgeElement edge : edgeSet())
			m_cost += edge.getCost();
		for (NodeElement node : vertexSet())
			m_cost += node.getCost();
		
		double cursorCost = 0.0;
		for (Cursor c : m_cursors)
			cursorCost += c.getCost();
//		log.debug(m_cost + " " + cursorCost);
		m_cost = cursorCost;

//		if (track)
//			log.debug(" " + m_cost + " " + m_valid);
		
//		NodeElement start = null;
//		for (NodeElement node : vertexSet()) {
//			if (inDegreeOf(node) + outDegreeOf(node) == 1) {
//				start = node;
//				break;
//			}
//		}	
//		if (start != null)
//			m_cost = getLongestPath(start, new ArrayList<String>());
//		else
//			m_cost = m_edges.size();
		
//		if (m_edges.size() == 3 && m_cost == 3)
//			log.debug("blah");
	}
	
	public boolean isValid() {
		return m_valid;
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
	
	public double getCost() {
		return m_cost;
	}

	public int compareTo(Subgraph o) {
		return ((Double)getCost()).compareTo(o.getCost());
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
		
//		addAugmentedEdges();

		m_label2var.putAll(m_rename);
		
		if (m_structuredNode != null && query == null)
			m_label2var.put(m_structuredNode.getLabel(), "?ATTACH");
		
		int x = 0;
		for (EdgeElement edge : edgeSet()) {
			String src = m_label2var.get(edge.getSource().getLabel());
			if (src == null) { // && !edge.getSource().equals(m_structuredNode)) {
				src = "?sx" + ++x;
				m_label2var.put(edge.getSource().getLabel(), src);
			}
			
			String trg = m_label2var.get(edge.getTarget().getLabel());
			if (trg == null) { // && !edge.getTarget().equals(m_structuredNode)) {
				trg = "?sx" + ++x;
				m_label2var.put(edge.getTarget().getLabel(), trg);
			}
		}

		// m_mappings contains ext->ext mappings from this subgraph to
		// isomorphic subgraphs found during exploration
		// create maps containing query var->extension mappings
//		List<String> columns = new ArrayList<String>();
//		for (String ext : m_label2var.keySet()) {
//			String v = m_label2var.get(ext);
//			if (Util.isVariable(v))
//				columns.add(v);
//		}
//		if (query != null) {
//			columns.add("?PLACEHOLDER");
//			m_label2var.put(m_structuredNode.getLabel(), "?PLACEHOLDER");
//		}
		
//		Table<String> indexMatches = new Table<String>(columns);
		
		// don't forget this subgraph
//		String[] row = new String[indexMatches.columnCount()];
//		for (String ext : m_label2var.keySet()) {
//			String v = m_label2var.get(ext);
//			if (Util.isVariable(v))
//				row[indexMatches.getColumn(v)] = ext;
//		}
//		indexMatches.addRow(row);
//		
//		// from the isomorphic subgraphs
//		for (Map<NodeElement,NodeElement> extMap : m_mappings) {
//			row = new String[indexMatches.columnCount()];
//			for (NodeElement node : extMap.keySet()) {
//				String ext = node.getLabel();
//				String v = m_label2var.get(ext);
//				if (v != null && Util.isVariable(v)) // v is null if the node is the connecting node to a structured query
//					row[indexMatches.getColumn(v)] = extMap.get(node).getLabel();
//			}
//			indexMatches.addRow(row);
//		}
//		
		// build a result table for each augmented edge
//		List<Table<String>> resultTables = new ArrayList<Table<String>>();
//		for (NodeElement augmentedNode : m_augmentedNodes) {
//			Set<KeywordSegment> kss = m_nodeSegments.get(augmentedNode);
//			assert kss.size() == 1;
//			for (KeywordSegment ks : kss) {
//				Table<String> table = new Table<String>(augmentedNode.getLabel(), ks.toString());
//				table.addRows(augmentedNode.getSegmentEntities(ks).getRows());
//				Set<String> alreadyAdded = new HashSet<String>();
//				for (Map<NodeElement,NodeElement> map : m_mappings)
//					if (alreadyAdded.add(map.get(augmentedNode).getLabel()))
//						table.addRows(map.get(augmentedNode).getSegmentEntities(ks).getRows());
//				resultTables.add(table);
//			}
//		}

//		if (query != null) {
//			// in the results of the structured query, a single variable
//			// may be mapped to different extensions in different answers
//			// if these extensions occur in the current subgraph as nodes, we can
//			// attach the structured query at multiple points
//			for (QNode var : ext2vars.get(m_structuredNode.getLabel())) {
//				// deep copy extension mappings, which will be extended by attachQuery
//				Table<String> copy = new Table<String>(indexMatches, false);
//				for (String[] r : indexMatches)
//					copy.addRow(r.clone());
//				queries.add(attachQuery(query, var, copy, resultTables));
//			}
//		}
//		else {
			TranslatedQuery q = new TranslatedQuery("qt", null);
			for (EdgeElement edge : edgeSet()) {
				if (Util.isVariable(m_label2var.get(edge.getTarget().getLabel())))
					q.addEdge(m_label2var.get(edge.getSource().getLabel()), edge.getLabel(), m_label2var.get(edge.getTarget().getLabel()));
				else {
					QNode qnode = m_keywordNodes.get(m_label2var.get(edge.getTarget().getLabel()));
					if (qnode == null)
						qnode = new QNode(edge.getTarget().getLabel());
					q.addAttributeEdge(new QNode(m_label2var.get(edge.getSource().getLabel())), edge.getLabel(), qnode);
				}
			}
			
			if (structuredEntities != null && structuredEntities.size() > 0) {
				Table<String> t = new Table<String>("?ATTACH");
				for (String s : structuredEntities)
					t.addRow(new String[] { s });
				q.addResult(t);
			}

//			q.setIndexMatches(indexMatches);
//			
//			for (Table<String> table : resultTables) {
//				Table<String> copy = new Table<String>(table, true);
//				copy.setColumnName(0, m_label2var.get(copy.getColumnName(0)));
//				q.addResult(copy);
//				q.setAsSelect(copy.getColumnName(0));
//			}
			
			for (QNode qn : q.getQueryGraph().vertexSet())
				if (qn.isVariable())
					q.setAsSelect(qn.getLabel());
			
			queries.add(q);
//		}
		
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
		String edges = "";
		String addComma = "";
		for (EdgeElement edge : edgeSet()) {
			edges += addComma + edge.getLabel() + "[" + edge.getCost() + "](" + edge.getSource().getLabel() + "," + edge.getTarget().getLabel() + ")";
			addComma = ",";
		}
		return "SG[" + edgeSet().size() + "," + m_cost + "," + edges + "]";
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
