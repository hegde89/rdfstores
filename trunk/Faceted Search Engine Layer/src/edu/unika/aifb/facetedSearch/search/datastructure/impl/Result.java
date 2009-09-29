package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import edu.unika.aifb.facetedSearch.search.datastructure.IResult;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 */
public class Result implements IResult {

	private Table<String> m_resultTable;
	private FacetPage m_facetPage;

	public Result() {
		m_resultTable = new Table<String>();
	}

	public Result(Table<String> resultTable) {
		m_resultTable = resultTable;
	}

	public FacetPage getFacetPage() {
		return m_facetPage;
	}

	public Table<String> getResultSubTable(int fromIndex, int toIndex) {
		return m_resultTable.subTable(fromIndex, toIndex);
	}

	public Table<String> getResultTable() {
		return m_resultTable;
	}

	public boolean hasFacetPage() {
		return m_facetPage != null;
	}

	public void setFacetPage(FacetPage facetPage) {
		m_facetPage = facetPage;
	}

	public void setResultTable(Table<String> resultTable) {
		m_resultTable = resultTable;
	}

	public int size() {
		return m_resultTable.size();
	}
}
