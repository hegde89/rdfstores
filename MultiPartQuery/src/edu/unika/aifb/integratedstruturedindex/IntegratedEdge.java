package edu.unika.aifb.integratedstruturedindex;

import java.util.LinkedList;

public class IntegratedEdge {
	private String label;
	IntegratedExtension iSrc;
	IntegratedExtension iTrg;
	LinkedList<String> ds = new LinkedList<String>();
	
	public void setLabel(String s) {
		this.label = s;
	}
	
	public void setSrc(IntegratedExtension src) {
		this.iSrc = src;
	}
	
	public void setTrg(IntegratedExtension trg) {
		this.iTrg = trg;
	}
	
	public void addDS(String s) {
		this.ds.add(s);
	}
}
