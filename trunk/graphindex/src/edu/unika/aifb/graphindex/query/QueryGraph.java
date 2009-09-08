package edu.unika.aifb.graphindex.query;

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

import java.util.HashSet;
import java.util.Set;

import org.jgrapht.graph.DirectedMultigraph;

public class QueryGraph extends DirectedMultigraph<QNode,QueryEdge> {
	private static final long serialVersionUID = -6174083887964946008L;

	public QueryGraph() {
		super(QueryEdge.class);
	}

	public QueryEdge addEdge(String src, String property, String trg) {
		return addEdge(new QNode(src), property, new QNode(trg));
	}
	
	public QueryEdge addEdge(QNode src, String property, QNode trg) {
		if (!containsVertex(src))
			addVertex(src);
		else
			src = getNodeByLabel(src.getLabel());
		
		if (!containsVertex(trg))
			addVertex(trg);
		else
			trg = getNodeByLabel(trg.getLabel());
		
		QueryEdge e = new QueryEdge(src, trg, property, this);
		addEdge(src, trg, e);
		return e;
	}
	
	public QNode getNodeByLabel(String label) {
		for (QNode node : vertexSet())
			if (node.getLabel().equals(label))
				return node;
		return null;
	}
	
	public int edgeCount() {
		return edgeSet().size();
	}
	
	public int nodeCount() {
		return vertexSet().size();
	}
	
	public int degreeOf(QNode node) {
		return inDegreeOf(node) + outDegreeOf(node);
	}
	
	public Set<QNode> predecessors(QNode node) {
		Set<QNode> preds = new HashSet<QNode>();
		for (QueryEdge e : incomingEdgesOf(node))
			preds.add(e.getSource());
		return preds;
	}

	public Set<QNode> successors(QNode node) {
		Set<QNode> preds = new HashSet<QNode>();
		for (QueryEdge e : outgoingEdgesOf(node))
			preds.add(e.getTarget());
		return preds;
	}
	
	public QueryGraph deepCopy() {
		QueryGraph graph = new QueryGraph();
		
		for (QueryEdge e : edgeSet()) {
			graph.addEdge((QNode)e.getSource().clone(), e.getLabel(), (QNode)e.getTarget().clone());
		}
		
		return graph;
	}
}
