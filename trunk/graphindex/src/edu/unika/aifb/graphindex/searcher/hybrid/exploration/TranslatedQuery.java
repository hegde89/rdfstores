package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.StructuredQuery;

public class TranslatedQuery extends StructuredQuery {
	private Table<String> m_indexMatches;
	private QNode m_connectingNode;
	private List<QueryEdge> m_structuredEdges;
	private List<QueryEdge> m_attributeEdges;
	private List<Table<String>> m_results;
	private Table<String> m_result;
	
	public TranslatedQuery(String name, QNode connectingNode) {
		super(name);
		m_connectingNode = connectingNode;
		m_results = new ArrayList<Table<String>>();
		m_structuredEdges = new ArrayList<QueryEdge>();
		m_attributeEdges = new ArrayList<QueryEdge>();
	}

	public void setIndexMatches(Table<String> matches) {
		m_indexMatches = matches;
	}
	
	public Table<String> getIndexMatches() {
		return m_indexMatches;
	}
	
	public QNode getConnectingNode() {
		return m_connectingNode;
	}
	
	public QueryEdge addEdge(String src, String property, String trg, boolean fromStructured) {
		QueryEdge e = super.addEdge(src, property, trg);
		if (fromStructured)
			m_structuredEdges.add(e);
		else if (property.startsWith("???"))
			m_attributeEdges.add(e);
		return e;
	}
	
	public QueryEdge addEdge(QNode src, String property, QNode trg, boolean fromStructured) {
		QueryEdge e = super.addEdge(src, property, trg);
		if (fromStructured)
			m_structuredEdges.add(e);
		else if (property.startsWith("???"))
			m_attributeEdges.add(e);
		return e;
	}

	public List<QueryEdge> getStructuredEdges() {
		return m_structuredEdges;
	}
	
	public List<QueryEdge> getAttributeEdges() {
		return m_attributeEdges;
	}
	
	public List<Table<String>> getResults() {
		return m_results;
	}
	
	public void addResult(Table<String> table) {
		m_results.add(table);
	}
	
	public void setResult(Table<String> result) {
		m_result = result;
	}

	public Table<String> getResult() {
		return m_result;
	}
}
