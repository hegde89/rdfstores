package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.log4j.Logger;
import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryEvaluator implements IQueryEvaluator {
	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private ExtensionManager m_em;
	private ExtensionStorage m_es;
	private Timings m_timings;
	private HashMap<String,IndexGraphMatcher> m_matchers;
	private IndexMatchesValidator m_mlv;
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public QueryEvaluator(StructureIndexReader indexReader) throws StorageException {
		m_indexReader = indexReader;
		m_index = indexReader.getIndex();
		m_em = m_index.getExtensionManager();
		m_em.setMode(ExtensionManager.MODE_READONLY);
		m_es = m_em.getExtensionStorage();
		m_matchers = new HashMap<String,IndexGraphMatcher>();
//		m_mlv = new MappingListValidator(m_index, m_index.getCollector());
		m_mlv = new SmallIndexMatchesValidator(m_index, m_index.getCollector());

		for (String ig : m_indexReader.getGraphNames()) {
//			IndexGraphMatcher matcher = new JoinMatcher(m_index, ig);
			IndexGraphMatcher matcher = new SmallIndexGraphMatcher(m_index, ig);
			matcher.initialize();
			m_matchers.put(ig, matcher);
		}
	}
	
	public IndexMatchesValidator getMLV() {
		return m_mlv;
	}
	
	public List<String[]> evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");

		m_index.getCollector().reset();
		m_timings = new Timings();
		m_index.getCollector().addTimings(m_timings);
		
		long start = System.currentTimeMillis();
		
		List<String[]> result = new ArrayList<String[]>();
		for (String indexGraph : m_indexReader.getGraphNames()) {
			QueryExecution qe = new QueryExecution(query, m_index);
			
			IndexGraphMatcher matcher = m_matchers.get(indexGraph);
			
			matcher.setTimings(m_timings);
			matcher.setQueryExecution(qe);

			GTable.timings = m_timings;
			Tables.timings = m_timings;
			
			m_timings.start(Timings.STEP_IM);
			matcher.match();
			m_timings.end(Timings.STEP_IM);
			
			m_index.getCollector().logStats();
			
			if (qe.getIndexMatches() == null || qe.getIndexMatches().rowCount() == 0)
				continue;
			
			m_mlv.setQueryExecution(qe);
			
			m_timings.start(Timings.STEP_DM);
			m_mlv.validateIndexMatches();
			m_timings.end(Timings.STEP_DM);

			m_index.getCollector().addTimings(m_mlv.getTimings());
			m_index.getCollector().logStats();

			if (qe.getResult().rowCount() > 0)
				result.addAll(qe.getResult().getRows());
			
			qe.finished();
		}
		log.debug("size: " + result.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);
		

		((LuceneExtensionStorage)m_es).logStats(log);
		
		return result;
	}
	
	public void clearCaches() throws StorageException {
		m_mlv.clearCaches();
		m_es.clearCaches();
		((LuceneExtensionStorage)m_es).reopenAndWarmUp();
	}

	public long[] getTimings() {
		m_index.getCollector().logStats();
		return m_index.getCollector().getConsolidated();
	}
}
