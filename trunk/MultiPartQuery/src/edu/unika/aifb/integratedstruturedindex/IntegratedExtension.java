package edu.unika.aifb.integratedstruturedindex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

public class IntegratedExtension {
	
	private Set<String> exts = new HashSet<String>();
	private long id = -1;
	
	

	public IntegratedExtension(long id) {
		this.id = id;
	}
	
	public void addExt(String ext) {
//		if (!this.exts.contains(ext)) {
			this.exts.add(ext);
//		}
		
	}
	
	public long getId() {
		return id;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((exts == null) ? 0 : exts.hashCode());
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
		IntegratedExtension other = (IntegratedExtension) obj;
		if (exts == null) {
			if (other.exts != null)
				return false;
		} else if (!exts.equals(other.exts))
			return false;
		return true;
	}

	public Iterator<String> iterator() {
		return exts.iterator();
	}
	
//	public LinkedList getList() {
//		return this.exts;
//	}
}
