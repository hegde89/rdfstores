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
package edu.unika.aifb.facetedSearch.store.impl;

import java.util.Iterator;

import org.apexlab.service.session.datastructure.ResultItem;
import org.apexlab.service.session.datastructure.ResultPage;
import org.apexlab.service.session.datastructure.Table;

import edu.unika.aifb.facetedSearch.Environment;
import edu.unika.aifb.facetedSearch.util.Util;

public class EvaluatorHelper {

	public static ResultPage constructResultPage(
			edu.unika.aifb.graphindex.data.Table<String> graphIndex_resultTable) {

		ResultPage resultPage = new ResultPage();
		Iterator<String[]> row_iter = graphIndex_resultTable.iterator();
		int columnCount = graphIndex_resultTable.columnCount();

		Table<ResultItem> hermes_resultTable = new Table<ResultItem>(
				graphIndex_resultTable.getColumnNames());

		while (row_iter.hasNext()) {

			ResultItem[] resultItemRow = new ResultItem[columnCount];
			String[] row = row_iter.next();

			for (int i = 0; i < row.length; i++) {

				ResultItem item = new ResultItem();
				item
						.setType(Util.isEntity(row[i]) ? Environment.ResultItemType.INDIVIDUAL
								: Environment.ResultItemType.LITERAL);

				item.setURL(row[i]);
				item.setTitle(Util.getLocalName(row[i]));

				resultItemRow[i] = item;
			}

			hermes_resultTable.addRow(resultItemRow);
		}

		resultPage.setResultItemTable(hermes_resultTable);

		return resultPage;
	}

}
