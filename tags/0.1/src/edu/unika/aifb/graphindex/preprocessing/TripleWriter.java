package edu.unika.aifb.graphindex.preprocessing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.unika.aifb.graphindex.importer.HashedTripleSink;
import edu.unika.aifb.graphindex.importer.TripleSink;

public class TripleWriter implements HashedTripleSink, TripleSink {

	private PrintWriter m_out;
	
	public TripleWriter(String file) throws IOException {
		m_out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
	}
	
	public void triple(long s, long p, long o) {
		m_out.println(s + "\t" + p + "\t" + o);
	}

	public void close() throws IOException {
		m_out.close();
	}

	public void triple(String s, String p, String o) {
		m_out.println(s + "\t" + p + "\t" + o);
	}
}
