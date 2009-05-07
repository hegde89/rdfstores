/**
 * 
 */
package edu.unika.aifb.graphindex.data;


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