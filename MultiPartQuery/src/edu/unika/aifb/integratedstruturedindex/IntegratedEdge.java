package edu.unika.aifb.integratedstruturedindex;

import java.util.HashSet;
import java.util.Set;

public class IntegratedEdge {
	private String label;
//	IntegratedExtension iSrc;
//	IntegratedExtension iTrg;
	Set<String> ds = new HashSet<String>();
	
	public void setLabel(String s) {
		this.label = s;
	}
	
	public String getLabel() {
		return this.label;
	}
	
//	public void setSrc(IntegratedExtension src) {
//		this.iSrc = src;
//	}
//	
//	public IntegratedExtension getiSrc() {
//		return iSrc;
//	}
//
//	public IntegratedExtension getiTrg() {
//		return iTrg;
//	}
//
//	public void setTrg(IntegratedExtension trg) {
//		this.iTrg = trg;
//	}
	
	
	
	public void addDS(String s) {
		if (!this.ds.add(s)) {
			System.out.println("DS " + s + " already in list of edge " + label + "!");
		}
	}

	
}
