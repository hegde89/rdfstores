package edu.unika.aifb.graphindex.searcher;

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

import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public abstract class Searcher {
	protected IndexReader m_idxReader;
	protected Timings m_timings;
	protected Counters m_counters;

	protected Searcher(IndexReader idxReader) {
		m_idxReader = idxReader;
		m_timings = new Timings();
		m_counters = new Counters();
		m_idxReader.getCollector().addTimings(m_timings);
		m_idxReader.getCollector().addCounters(m_counters);
	}
}
