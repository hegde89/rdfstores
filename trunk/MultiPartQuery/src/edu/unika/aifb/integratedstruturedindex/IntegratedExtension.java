package edu.unika.aifb.integratedstruturedindex;

import java.util.Iterator;
import java.util.LinkedList;

public class IntegratedExtension {
	
	private LinkedList<String> exts = new LinkedList<String>();
	
	public void addExt(String ext) {
		if (!this.exts.contains(ext)) {
			this.exts.add(ext);
		}
		
	}
	
	public Iterator<String> iterator() {
		return exts.iterator();
	}
	
	public LinkedList getList() {
		return this.exts;
	}
}
