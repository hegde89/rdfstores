package edu.unika.aifb.graphindex.searcher.keyword;

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

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.storage.StorageException;

public abstract class KeywordQueryEvaluator extends Searcher {

	protected KeywordQueryEvaluator(IndexReader idxReader) {
		super(idxReader);
	}

	public abstract GTable<String> evaluate(KeywordQuery query) throws StorageException, IOException;
}
