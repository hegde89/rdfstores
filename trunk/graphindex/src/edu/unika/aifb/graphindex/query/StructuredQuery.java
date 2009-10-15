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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.jgrapht.graph.DirectedMultigraph;


public class StructuredQuery extends Query {
	protected QueryGraph m_queryGraph;
	protected List<QNode> m_selectVariables;
	protected List<QNode> m_variables;
	
	public StructuredQuery(String name) {
		super(name);
		
		m_queryGraph = new QueryGraph();
		m_selectVariables = new ArrayList<QNode>();
	}

	
	public QueryEdge addEdge(String src, String property, String trg) {
		return m_queryGraph.addEdge(src, property, trg);
	}
	
	public QueryEdge addEdge(QNode src, String property, QNode trg) {
		return m_queryGraph.addEdge(src, property, trg);
	}

	public QueryGraph getQueryGraph() {
		return m_queryGraph;
	}
	
//	public void setSelectVariables(List<String> selectVariables) {
//		for (String var : selectVariables) {
//			QNode node = m_queryGraph.getNodeByLabel(var);
//			if (node != null) {
//				node.setSelectVariable(true);
//				m_selectVariables.add(node);
//			}
//		}
//	}
	
	public QNode getNode(String label) {
		for (QNode node : m_queryGraph.vertexSet())
			if (node.equals(label))
				return node;
		return null;
	}
	
	public void setAsSelect(String varLabel) {
		QNode node = m_queryGraph.getNodeByLabel(varLabel);
		node.setSelectVariable(true);
		m_selectVariables.add(node);
	}
	
	public List<QNode> getSelectVariables() {
		return m_selectVariables;
	}
	
	public List<QNode> getVariables() {
		if (m_variables == null) {
			m_variables = new ArrayList<QNode>();
			for (QNode node : m_queryGraph.vertexSet())
				if (node.isVariable())
					m_variables.add(node);
		}
		return m_variables;
	}

	public List<String> getSelectVariableLabels() {
		List<String> labels = new ArrayList<String>();
		for (QNode node : m_selectVariables)
			labels.add(node.getLabel());
		return labels;
	}

	public Map<String,Integer> calculateConstantProximities() {
		QNode startNode = null;
		final Map<String,Integer> scores = new HashMap<String,Integer>();
		for (QNode node : m_queryGraph.vertexSet()) {
			if (node.isConstant()) {
				scores.put(node.getLabel(), 0);
				startNode = node;
			}
		}
		
		if (startNode == null) {
			// no constants, set all scores to zero
			for (QNode node : m_queryGraph.vertexSet())
				scores.put(node.getLabel(), 0);
			return scores;
		}
		
		Stack<QNode> tov = new Stack<QNode>();
		Set<QNode> visited = new HashSet<QNode>();
		
		tov.push(startNode);
		
		while (tov.size() > 0) {
			QNode node = tov.pop();
			
			if (visited.contains(node))
				continue;
			visited.add(node);

			String curNode = node.getLabel();
			
			int min = Integer.MAX_VALUE;
			for (QNode n : m_queryGraph.predecessors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = n.getLabel();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(n))
					tov.push(n);
			}
			
			for (QNode n : m_queryGraph.successors(node)) {
				if (!scores.containsKey(curNode)) {
					String v = n.getLabel();
					if (scores.containsKey(v) && scores.get(v) < min)
						min = scores.get(v);
				}
				if (!visited.contains(n))
					tov.push(n);
			}
			
			if (!scores.containsKey(curNode))
				scores.put(curNode, min + 1);
		}
		
		return scores;
	}


	public void setName(String queryName) {
		m_name = queryName;
	}
	
	public String toString() {
		String s = "structured query " + m_name + ", select: " + getSelectVariableLabels() + "\n";
		String add = "";
		for (QueryEdge e : m_queryGraph.edgeSet()) {
			s += add + e.getSource() + " " + e.getLabel() + " "  + e.getTarget();
			add = "\n";
		}
				
		return s;
	}
}
