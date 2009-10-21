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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.FacetEnvironment.EdgeType;
import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.FacetTreeDelegator;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.DynamicNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Edge;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.FacetValueNode;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.Node;
import edu.unika.aifb.facetedSearch.facets.tree.model.impl.StaticNode;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.Result;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.Delegators;
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
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

	/*
	 * 
	 */
	private HashSet<String> m_parsedItems;

	public FacetRequestHelper(SearchSession session) {

		m_session = session;
		m_treeDelegator = (FacetTreeDelegator) session
				.getDelegator(Delegators.TREE);
		m_parsedItems = new HashSet<String>();
	}

	@SuppressWarnings("unused")
	private Table<String> getAdditionalTable(StaticNode node,
			List<String> sources, StructuredQuery sQuery) {

		String domain = node.getDomain();

		ArrayList<String> columnNames = new ArrayList<String>();
		columnNames.addAll(FacetUtils.getColumnsNames4Table(sQuery));

		ArrayList<String[]> rows = new ArrayList<String[]>();

		Table<String> additionalTable = new Table<String>(columnNames);
		Stack<Edge> path = m_treeDelegator.getPathFromRoot(node);

		Edge edge;
		Node src;
		Node tar;

		int colSize = columnNames.size();

		for (String source : sources) {

			Stack<Edge> pathCopy = new Stack<Edge>();
			pathCopy.addAll(path);

			int countColumn = 0;

			ArrayList<String[]> newRows = new ArrayList<String[]>();
			String[] newRow = new String[colSize];

			newRow[countColumn] = source;
			newRows.add(newRow);

			countColumn++;

			while (!pathCopy.isEmpty()) {

				edge = pathCopy.pop();
				src = edge.getSource();

				if (src.isSubTreeRoot()) {

					while (edge.getType() != EdgeType.HAS_RANGE) {
						edge = pathCopy.pop();
					}

					if (pathCopy.isEmpty()) {

						tar = edge.getTarget();

						Set<AbstractSingleFacetValue> fvs = ((StaticNode) tar)
								.getObjects();

						if (fvs.size() > 0) {

							for (AbstractSingleFacetValue fv : fvs) {

								Collection<String> fvSources = m_session
										.getCache().getSources4Object(domain,
												fv.getValue());

								if (fvSources.contains(source)) {

									ArrayList<String[]> newRowsCopy = new ArrayList<String[]>();

									for (String[] row : newRows) {

										String[] rowCopy = new String[colSize];
										System.arraycopy(row, 0, rowCopy, 0,
												colSize);

										rowCopy[countColumn] = fv.getValue();
										newRowsCopy.add(rowCopy);
									}

									newRows.clear();
									newRows.addAll(newRowsCopy);

								}
							}

							countColumn++;
						}
					} else {

						edge = pathCopy.pop();

						while (!pathCopy.isEmpty()
								&& (edge.getType() == EdgeType.SUBCLASS_OF)) {
							edge = pathCopy.pop();
						}

						if (pathCopy.isEmpty()) {

							tar = edge.getTarget();

							if (tar instanceof FacetValueNode) {

								String value = ((FacetValueNode) tar)
										.getValue();

								ArrayList<String[]> newRowsCopy = new ArrayList<String[]>();

								for (String[] row : newRows) {

									String[] rowCopy = new String[colSize];
									System.arraycopy(row, 0, rowCopy, 0,
											colSize);

									rowCopy[countColumn] = value;
									newRowsCopy.add(rowCopy);
								}

								newRows.clear();
								newRows.addAll(newRowsCopy);

								countColumn++;

							} else if (tar instanceof DynamicNode) {

								List<AbstractSingleFacetValue> lits = ((DynamicNode) tar)
										.getLiterals();

								if (lits.size() > 0) {

									for (AbstractSingleFacetValue fv : lits) {

										Collection<String> fvSources = m_session
												.getCache().getSources4Object(
														domain, fv.getValue());

										if (fvSources.contains(source)) {

											ArrayList<String[]> newRowsCopy = new ArrayList<String[]>();

											for (String[] row : newRows) {

												String[] rowCopy = new String[colSize];
												System.arraycopy(row, 0,
														rowCopy, 0, colSize);

												rowCopy[countColumn] = fv
														.getValue();
												newRowsCopy.add(rowCopy);
											}

											newRows.clear();
											newRows.addAll(newRowsCopy);
										}
									}

									countColumn++;
								}

							} else {

								Set<AbstractSingleFacetValue> fvs = ((StaticNode) tar)
										.getObjects();

								if (fvs.size() > 0) {

									for (AbstractSingleFacetValue fv : fvs) {

										Collection<String> fvSources = m_session
												.getCache().getSources4Object(
														domain, fv.getValue());

										if (fvSources.contains(source)) {

											ArrayList<String[]> newRowsCopy = new ArrayList<String[]>();

											for (String[] row : newRows) {

												String[] rowCopy = new String[colSize];
												System.arraycopy(row, 0,
														rowCopy, 0, colSize);

												rowCopy[countColumn] = fv
														.getValue();
												newRowsCopy.add(rowCopy);
											}

											newRows.clear();
											newRows.addAll(newRowsCopy);
										}
									}

									countColumn++;
								}
							}
						} else {

							pathCopy.push(edge);
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

	public List<String> getSourcesList(Table<String> newTable, String domain) {

		List<String> sources = new ArrayList<String>();

		int column = newTable.getColumn(domain);
		Iterator<String[]> rowIter = newTable.getRows().iterator();

		while (rowIter.hasNext()) {

			String rowItem = rowIter.next()[column];

			if (!m_parsedItems.contains(rowItem)) {
				sources.add(rowItem);
				m_parsedItems.add(rowItem);
			}
		}

		m_parsedItems.clear();
		Collections.sort(sources);

		return sources;
	}

	public Table<String> refineResult(String domain, List<String> sources)
			throws DatabaseException, IOException {

		Collections.sort(sources);
		// Table<String> addTable = getAdditionalTable(node, sources, sQuery);

		Table<String> oldTable = m_session.getCache().getCurrentResultTable();
		oldTable.sort(domain, true);

		return FacetUtils.mergeJoin(oldTable, sources, domain);
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
