package edu.unika.aifb.keywordsearch;

import java.io.Serializable;

public class StructureEdge implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public static final String RANGE_EDGE = "range_edge";
	
	public static final String DOMAIN_EDGE = "domain_edge";

	private SummaryGraphElement m_source = null;
	private SummaryGraphElement m_target = null;
	
	private String m_edgeLabel = "";
	
	public StructureEdge(SummaryGraphElement source, SummaryGraphElement target, String type){

		if (type.equals(RANGE_EDGE)) m_edgeLabel = RANGE_EDGE;
		else if (type.equals(DOMAIN_EDGE)) m_edgeLabel = DOMAIN_EDGE;

		m_source = source; 
		m_target = target;
	}
	
	public boolean equals(Object object){
		if(this == object) return true;
		if(object == null) return false;
		if(!(object instanceof StructureEdge)) return false;
		StructureEdge edge = (StructureEdge)object;
		
		if (!m_edgeLabel.equals(edge.getEdgeLabel()))  return false;
		if (!getSource().equals(edge.getSource())) return false;
		if (!getTarget().equals(edge.getTarget())) return false;
		
		return true;
	}

	public int hashCode(){
		return 7 * getSource().hashCode() 
			+ 11 * getTarget().hashCode() 
			+ 13 * getEdgeLabel().hashCode();
	}
	
	public String toString(){
		if(getSource() != null && getTarget() != null && getEdgeLabel() != null) 
			return getSource().toString() + "["+getSource().getEF()+"]["+getSource().getMatchingScore()+"]["+getSource().getTotalCost()+"] " + getEdgeLabel() + " "  + getTarget().toString()+"["+getTarget().getEF()+"]["+getTarget().getMatchingScore()+"]["+getTarget().getTotalCost()+"]";
		else return super.toString();
	}

	public SummaryGraphElement getSource() {
		return m_source;
	}

	public void setSource(SummaryGraphElement m_source) {
		this.m_source = m_source;
	}

	public SummaryGraphElement getTarget() {
		return m_target;
	}

	public void setTarget(SummaryGraphElement m_target) {
		this.m_target = m_target;
	}

	public String getEdgeLabel() {
		return m_edgeLabel;
	}

	public void setEdgeLabel(String label) {
		m_edgeLabel = label;
	}

}
