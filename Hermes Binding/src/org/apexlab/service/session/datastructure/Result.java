package org.apexlab.service.session.datastructure;

/**
 * This class represents all the results returned by the search engine for a
 * given query, independently from the page to display
 * 
 * @author tpenin
 */
public class Result {

	// The list of all the result items that were found by the search engine
	public Table<ResultItem> m_resultItemTable;
	// The list of all the sources that have contributed to the results
	public Source m_source;

	/**
	 * Default constructor
	 */
	public Result() {
		this.m_resultItemTable = new Table<ResultItem>();
		this.m_source = null;
	}

	/**
	 * Constructor
	 * 
	 * @param resultItemList
	 *            The result items that were found by the search engine
	 * @param source
	 *            The current source
	 */
	public Result(Table<ResultItem> resultTable, Source source) {
		this.m_resultItemTable = resultTable;
		this.m_source = source;
	}

	/**
	 * @return the resultItemList
	 */
	public Table<ResultItem> getResultItemTable() {
		return this.m_resultItemTable;
	}

	/**
	 * @return the source
	 */
	public Source getSource() {
		return this.m_source;
	}

	/**
	 * @param resultItemList
	 *            the resultItemList to set
	 */
	public void setResultItemTable(Table<ResultItem> resultTable) {
		this.m_resultItemTable = resultTable;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public void setSource(Source source) {
		this.m_source = source;
	}

}
