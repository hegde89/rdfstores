package edu.unika.aifb.graphindex.importer;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.GraphBuilder;

public abstract class Importer {

	protected GraphBuilder m_gb;
	protected List<String> m_files;

	protected static Logger log;
	
	protected Importer() {
		m_files = new ArrayList<String>();
	}
	
	public void addImport(String fileName) {
		m_files.add(fileName);
	}

	public void setGraphBuilder(GraphBuilder gb) {
		m_gb = gb;
	}

	public abstract void doImport();
}
