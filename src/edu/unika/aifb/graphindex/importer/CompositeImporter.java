package edu.unika.aifb.graphindex.importer;

import java.util.LinkedList;
import java.util.List;

public class CompositeImporter extends Importer {

	private List<Importer> m_importers;
	
	public CompositeImporter() {
		m_importers = new LinkedList<Importer>();
	}
	
	public void addImporter(Importer importer) {
		m_importers.add(importer);
	}
	
	@Override
	public void doImport() {
		for (Importer importer : m_importers) {
			importer.setTripleSink(m_sink);
			importer.doImport();
		}
	}

}
