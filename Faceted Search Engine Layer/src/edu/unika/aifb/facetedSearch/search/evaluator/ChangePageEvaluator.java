/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.search.evaluator;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.search.datastructure.ChangePageRequest;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.ResultPage;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.searcher.Searcher;

/**
 * @author andi
 * 
 */
public class ChangePageEvaluator extends Searcher {

	private SearchSession m_session;

	public ChangePageEvaluator(IndexReader idxReader, SearchSession session) {

		super(idxReader);
		m_session = session;
	}

	public ResultPage evaluate(ChangePageRequest pageQuery) {

		Table<String> res4Page = null;
		ResultPage resPage = null;

		try {

			if ((res4Page = m_session.getCache().getResults4Page(
					pageQuery.getPage())) != null) {
				resPage = new ResultPage(res4Page, pageQuery.getPage());
			}

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return resPage == null ? ResultPage.EMPTY_PAGE : resPage;
	}
}
