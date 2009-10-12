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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.StructuredQuery;

/**
 * @author andi
 * 
 */
public class FacetRequestHelper {

	/*
	 * 
	 */
	private static Logger s_log = Logger.getLogger(FacetRequestHelper.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	/*
	 * 
	 */
	private FacetTreeDelegator m_treeDelegator;

	public FacetRequestHelper(SearchSession session) {

		m_session = session;
		m_treeDelegator = (FacetTreeDelegator) session
				.getDelegator(Delegators.TREE);
	}

	private Table<String> getAdditionalTable(StaticNode node,
			List<String> sources, StructuredQuery sQuery) {

		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String[]> rows = new ArrayList<String[]>();

		List<QNode> vars = sQuery.getVariables();

		for (QNode var : vars) {
			columnNames.add(var.getLabel());
		}

		Table<String> additionalTable = new Table<String>(columnNames);
		Stack<Edge> path = m_treeDelegator.getPathFromRoot(node);

		Edge edge;
		Node src;
		Node tar;

		int colSize = columnNames.size();

		for (String source : sources) {

			int countColumn = 0;

			ArrayList<String[]> newRows = new ArrayList<String[]>();
			String[] newRow = new String[colSize];

			newRow[countColumn] = source;
			newRows.add(newRow);

			countColumn++;

			while (!path.isEmpty()) {

				edge = path.pop();
				src = edge.getSource();

				if (src.isSubTreeRoot()) {

					while (edge.getType() != EdgeType.HAS_RANGE) {
						edge = path.pop();
					}

					if (path.isEmpty()) {

						tar = edge.getTarget();

						Set<AbstractSingleFacetValue> fvs = ((StaticNode) tar)
								.getObjects();

						if (fvs.size() > 0) {

							for (String[] row : newRows) {

								newRows.remove(row);

								for (AbstractSingleFacetValue fv : fvs) {

									String[] rowCopy = new String[colSize];
									System.arraycopy(row, 0, rowCopy, 0,
											colSize);

									rowCopy[countColumn] = fv.getValue();
									newRows.add(rowCopy);
								}
							}

							countColumn++;
						}
					} else {

						edge = path.pop();

						while (!path.isEmpty()
								&& (edge.getType() == EdgeType.SUBCLASS_OF)) {
							edge = path.pop();
						}

						if (path.isEmpty()) {

							tar = edge.getTarget();

							Set<AbstractSingleFacetValue> fvs = ((StaticNode) tar)
									.getObjects();

							if (fvs.size() > 0) {

								for (String[] row : newRows) {

									newRows.remove(row);

									for (AbstractSingleFacetValue fv : fvs) {

										String[] rowCopy = new String[colSize];
										System.arraycopy(row, 0, rowCopy, 0,
												colSize);

										rowCopy[countColumn] = fv.getValue();
										newRows.add(rowCopy);
									}
								}

								countColumn++;
							}
						} else {

							path.push(edge);
						}
					}
				} else {
					s_log.error("structure not correct for tree :"
							+ m_treeDelegator.getTree(node.getDomain()));
				}
			}

			rows.addAll(newRows);
		}

		additionalTable.setRows(rows);
		additionalTable.setSortedColumn(0);

		return additionalTable;
	}
	public Table<String> refineResult(StaticNode node, List<String> sources,
			StructuredQuery sQuery) throws DatabaseException, IOException {

		Collections.sort(sources);
		Table<String> addTable = getAdditionalTable(node, sources, sQuery);

		Table<String> oldTable = m_session.getCache().getCurrentResultTable();
		oldTable.sort(node.getDomain(), true);

		return Tables.mergeJoin(oldTable, addTable, node.getDomain());
	}

	public Result refineResult(Table<String> refinementTable, String domain) {

		Result res = new Result();
		refinementTable.sort(domain);

		try {

			Table<String> oldTable = m_session.getCache()
					.getCurrentResultTable();
			oldTable.sort(domain, true);

			res.setResultTable(Tables.mergeJoin(oldTable, refinementTable,
					domain));

		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return res;
	}

	public void updateColumns(Table<String> newTable, FacetedQuery fquery) {

		String[] names = newTable.getColumnNames();

		for (String name : names) {

			if (fquery.getOldVar2newVarMap().containsKey(name)) {

				String newName = fquery.getOldVar2newVarMap().get(name);
				newTable.setColumnName(newTable.getColumn(name), newName);
			}
		}
	}
}
