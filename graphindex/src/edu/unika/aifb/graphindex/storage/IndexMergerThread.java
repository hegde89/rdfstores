package edu.unika.aifb.graphindex.storage;

import java.io.File;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.storage.lucene.LuceneWarmer;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Util;

public class IndexMergerThread extends Thread {

	IndexDirectory m_idxDirectory;
	IndexDescription m_idx;
	private final static Logger log = Logger.getLogger(IndexMergerThread.class);
	ThreadListener listener;

	public IndexMergerThread(IndexDirectory idxDir, IndexDescription idx,
			ThreadListener listener) {
		super();
		m_idx = idx;
		m_idxDirectory = idxDir;
		this.listener = listener;
	}
	
	public void run() {

		log.debug("Starting merge thread for index: " + m_idx.toString());

		IndexStorage is;
		try {
			is = new LuceneIndexStorage(new File(m_idxDirectory.getDirectory(
					IndexDirectory.VP_DIR, false).getAbsolutePath()
					+ "/" + m_idx.getIndexFieldName()),
					new StatisticsCollector());

			is.initialize(false, false);

			log.debug("merging " + m_idx.toString());
			is.mergeSingleIndex(m_idx);
			is.close();

			Util.writeEdgeSet(m_idxDirectory.getDirectory(
					IndexDirectory.VP_DIR, false)
					+ "/" + m_idx.getIndexFieldName() + "_warmup", LuceneWarmer
					.getWarmupTerms(m_idxDirectory.getDirectory(
							IndexDirectory.VP_DIR, false)
							+ "/" + m_idx.getIndexFieldName(), 10));

			log.debug("Index " + m_idx.toString() + " merged.");
		} catch (Exception e) {
			e.printStackTrace();
		}

		listener.threadFinished(this);
	}

}
