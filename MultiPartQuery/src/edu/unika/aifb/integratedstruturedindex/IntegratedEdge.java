package edu.unika.aifb.integratedstruturedindex;

import java.util.HashSet;
import java.util.Set;

public class IntegratedEdge {
	private String label;
	IntegratedExtension iSrc;
	IntegratedExtension iTrg;
	Set<String> ds = new HashSet<String>();
	
	public void setLabel(String s) {
		this.label = s;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public void setSrc(IntegratedExtension src) {
		this.iSrc = src;
	}
	
	public IntegratedExtension getiSrc() {
		return iSrc;
	}

	public IntegratedExtension getiTrg() {
		return iTrg;
	}

	public void setTrg(IntegratedExtension trg) {
		this.iTrg = trg;
	}
	
	public void addDS(String s) {
		if (!this.ds.add(s)) {
			System.out.println("DS " + s + " already in list of edge " + label + "!");
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ds == null) ? 0 : ds.hashCode());
		result = prime * result + ((iSrc == null) ? 0 : iSrc.hashCode());
		result = prime * result + ((iTrg == null) ? 0 : iTrg.hashCode());
		result = prime * result + ((label == null) ? 0 : label.hashCode());
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
		if (ds == null) {
			if (other.ds != null)
				return false;
		} else if (!ds.equals(other.ds))
			return false;
		if (iSrc == null) {
			if (other.iSrc != null)
				return false;
		} else if (!iSrc.equals(other.iSrc))
			return false;
		if (iTrg == null) {
			if (other.iTrg != null)
				return false;
		} else if (!iTrg.equals(other.iTrg))
			return false;
		if (label == null) {
			if (other.label != null)
				return false;
		} else if (!label.equals(other.label))
			return false;
		return true;
	}
}
