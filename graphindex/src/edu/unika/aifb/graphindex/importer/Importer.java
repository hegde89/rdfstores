package edu.unika.aifb.graphindex.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.GraphBuilder;
import edu.unika.aifb.graphindex.TripleSink;

public abstract class Importer {

	protected TripleSink m_sink;
	protected List<String> m_files;

	protected static Logger log;
	
	protected Importer() {
		m_files = new ArrayList<String>();
	}
	
	public void addImport(String fileName) {
		m_files.add(fileName);
	}
	
	public void addImports(Collection<String> fileNames) {
		m_files.addAll(fileNames);
	}

	public void setTripleSink(TripleSink gb) {
		m_sink = gb;
	}

	public abstract void doImport();
}