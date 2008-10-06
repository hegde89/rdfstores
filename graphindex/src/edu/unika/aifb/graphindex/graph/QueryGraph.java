package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryGraph extends IndexGraph {

	private List<String>[] m_members;
	
	private final List<String> m_emptyList = new ArrayList<String>();
	
	@SuppressWarnings("unchecked")
	public QueryGraph(NamedGraph<String,LabeledEdge<String>> graph, Map<String,List<String>> members) {
		super(graph);
		m_members = new List [nodeCount()];
		
		for (int i = 0; i < m_members.length; i++)
			m_members[i] = members.get(getNodeLabel(i));
	}

	private void addMember(int node, String v) {
		List<String> members = m_members[node];
		if (members == null) {
			members = new ArrayList<String>();
			m_members[node] = members;
		}
		members.add(v);
	}
	
	public List<String> getMembers(int node) {
		List<String> members = m_members[node];
		if (members == null)
			return m_emptyList ;
		return members;
	}
}
