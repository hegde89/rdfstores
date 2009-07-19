package edu.unika.aifb.graphindex.searcher.structured.sig;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.IOException;
import java.util.List;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;

public abstract class AbstractIndexMatchesValidator implements IndexMatchesValidator {
	protected QueryExecution m_qe;
	protected StatisticsCollector m_collector;
	protected Timings t;
	protected Counters m_counters;
	protected IndexReader m_idxReader;
	
	public AbstractIndexMatchesValidator(IndexReader idxReader) throws IOException {
		m_idxReader = idxReader;
		m_collector = idxReader.getCollector();
		t = new Timings();

		if (!isCompatibleWithIndex())
			throw new UnsupportedOperationException("this index matcher is incompatible with the index");
	}

	public Timings getTimings() {
		return t;
	}
	
	public void setTimings(Timings timings) {
		t = timings;
	}
	
	public void setCounters(Counters c) {
		m_counters = c;
	}
	
	public void setQueryExecution(QueryExecution qe) {
		m_qe = qe;
	}
	
	protected abstract boolean isCompatibleWithIndex() throws IOException;
}
