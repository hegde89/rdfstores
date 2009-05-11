package edu.unika.aifb.graphindex.importer;

public interface TripleSink {
	public void triple(String s, String p, String o, String objectType);
}
