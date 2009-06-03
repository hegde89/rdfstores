package edu.unika.aifb.graphindex.importer;

import java.util.Iterator;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;

public class LuceneTripleImporter extends Importer {

	@Override
	public void doImport() {
		if (m_files.size() != 1)
			throw new UnsupportedOperationException("only one dir");
		
		try {
			LuceneGraphStorage gs = new LuceneGraphStorage(m_files.get(0));
			gs.initialize(false, true);
			gs.setStoreGraphName(false);
			
			for (Iterator<String[]> i = gs.iterator(); i.hasNext(); ) {
				String[] triple = i.next();
				m_sink.triple(triple[0], triple[1], triple[2], null);
			}
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

}
