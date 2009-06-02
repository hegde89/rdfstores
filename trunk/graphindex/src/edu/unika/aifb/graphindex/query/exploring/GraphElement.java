package edu.unika.aifb.graphindex.query.exploring;

import java.util.ArrayList;
import java.util.List;

public abstract class GraphElement {
	private String m_label;
	private List<Cursor> m_cursors;
	
	public GraphElement(String label) {
		m_label = label;
		m_cursors = new ArrayList<Cursor>();
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public void addCursor(Cursor c) {
		m_cursors.add(c);
	}
	
	public abstract List<GraphElement> getNeighbors(GraphElement parentToExclude);
}
