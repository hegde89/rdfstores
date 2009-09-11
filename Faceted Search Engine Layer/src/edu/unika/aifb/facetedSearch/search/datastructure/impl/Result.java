package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import edu.unika.aifb.graphindex.data.Table;

/**
 * This class represents all the results returned by the search engine for a
 * given query, independently from the page to display
 * 
 * @author tpenin
 */
public class Result {

	// The list of all the result items that were found by the search engine
	public Table<String> m_resultItemTable;

	/**
	 * Default constructor
	 */
	public Result() {
		this.m_resultItemTable = new Table<String>();
	}

	public Result(Table<String> resultTable) {
		this.m_resultItemTable = resultTable;
	}

	/**
	 * @return the resultItemList
	 */
	public Table<String> getResultItemTable() {
		return this.m_resultItemTable;
	}

	/**
	 * @param resultItemList
	 *            the resultItemList to set
	 */
	public void setResultItemTable(Table<String> resultTable) {
		this.m_resultItemTable = resultTable;
	}
}
