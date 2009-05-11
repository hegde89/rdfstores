package edu.unika.aifb.graphindex.query;

import java.util.List;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public abstract class AbstractIndexMatchesValidator implements IndexMatchesValidator {
	protected StructureIndex m_index;
	protected StatisticsCollector m_collector;
	protected ExtensionManager m_em;
	protected ExtensionStorage m_es;
	protected Timings t;
	
	public AbstractIndexMatchesValidator(StructureIndex index, StatisticsCollector collector) {
		m_index = index;
		m_collector = collector;
		m_em = m_index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
		t = new Timings();

		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("this index matcher is incompatible with the index");
	}

	protected abstract boolean isCompatibleWithIndex();
}
