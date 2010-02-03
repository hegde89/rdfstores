package edu.unika.aifb.integratedstruturedindex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class IntegratedEdge {
	private String label;
	private Set<String> ds = new HashSet<String>();
	private String srcID;
	private String trgID;
	
	
	public IntegratedEdge(String lbl, String src, String trg) {
		this.label = lbl;
		this.srcID = src;
		this.trgID = trg;
	}
	
	public void setLabel(String s) {
		this.label = s;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public Iterator<String> iterator() {
		return ds.iterator();
	}
	
	public boolean addDS(String s) {
		return this.ds.add(s);
//			System.out.println("DS " + s + " already in list of edge " + label + "!");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((label == null) ? 0 : label.hashCode());
		result = prime * result + ((srcID == null) ? 0 : srcID.hashCode());
		result = prime * result + ((trgID == null) ? 0 : trgID.hashCode());
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
		IntegratedEdge other = (IntegratedEdge) obj;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		if (srcID == null) {
			if (other.srcID != null)
				return false;
		} else if (!srcID.equals(other.srcID))
			return false;
		if (trgID == null) {
			if (other.trgID != null)
				return false;
		} else if (!trgID.equals(other.trgID))
			return false;
		return true;
	}
	
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((label == null) ? 0 : label.hashCode());
//		return result;
//	}
//
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		IntegratedEdge other = (IntegratedEdge) obj;
//		if (label == null) {
//			if (other.label != null)
//				return false;
//		} else if (!label.equals(other.label))
//			return false;
//		return true;
//	}
}
