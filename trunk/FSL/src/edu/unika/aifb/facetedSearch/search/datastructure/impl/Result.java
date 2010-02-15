package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import java.io.Serializable;

import edu.unika.aifb.facetedSearch.search.datastructure.IResult;
import edu.unika.aifb.facetedSearch.search.datastructure.impl.query.FacetedQuery;
import edu.unika.aifb.facetedSearch.search.fpage.impl.FacetPage;
import edu.unika.aifb.graphindex.data.Table;

/**
 * @author andi
 */
public class Result implements IResult, Serializable {

	/*
	 * 
	 */
	private static final String NO_ERROR = "no_error";

	/**
	 * 
	 */
	private static final long serialVersionUID = -4386192143288472710L;

	/*
	 * 
	 */
	private Table<String> m_resultTable;

	/*
	 * 
	 */
	private FacetPage m_facetPage;

	/*
	 * 
	 */
	private FacetedQuery m_query;

	/*
	 * 
	 */
	private String m_error;

	public Result() {

		m_resultTable = new Table<String>();
		setError(Result.NO_ERROR);
	}

	public Result(Table<String> resultTable) {

		m_resultTable = resultTable;
		setError(Result.NO_ERROR);
	}

	public void clearError() {
		setError(Result.NO_ERROR);
	}

	public String getError() {
		return m_error;
	}

	public FacetPage getFacetPage() {
		return m_facetPage;
	}

	public FacetedQuery getQuery() {
		return m_query;
	}

	public Table<String> getResultSubTable(int fromIndex, int toIndex) {

		return m_resultTable.subTable(fromIndex, toIndex);
	}

	public Table<String> getResultTable() {
		return m_resultTable;
	}

	public boolean hasError() {
		return !m_error.equals(Result.NO_ERROR);
	}

	public boolean hasFacetPage() {
		return m_facetPage != null;
	}

	public void setError(String error) {
		m_error = error;
	}

	public void setFacetPage(FacetPage facetPage) {
		m_facetPage = facetPage;
	}

	public void setQuery(FacetedQuery query) {
		m_query = query;
	}

	public void setResultTable(Table<String> resultTable) {
		m_resultTable = resultTable;
	}

	public int size() {
		return m_resultTable.rowCount();
	}
}
