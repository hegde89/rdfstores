package edu.unika.aifb.graphindex.searcher.structured.sig;

/**
 * Copyright (C) 2009 G�nter Ladwig (gla at aifb.uni-karlsruhe.de)
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
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public interface IndexMatchesValidator {
	public void setQueryExecution(QueryExecution qe);
	public void validateIndexMatches() throws StorageException, IOException;
	public void clearCaches();
	public Timings getTimings();
	public void setTimings(Timings timings);
	public void setCounters(Counters c);
}
