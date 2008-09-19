/**
 * 
 */
package edu.unika.aifb.graphindex;

import edu.unika.aifb.graphindex.storage.Triple;

public class TriplePair {
	public Triple target;
	public Triple source;
	
	public TriplePair(Triple target, Triple source) {
		this.target = target;
		this.source = source;
	}
	
	public String toString() {
		return target + "," + source;
	}
}