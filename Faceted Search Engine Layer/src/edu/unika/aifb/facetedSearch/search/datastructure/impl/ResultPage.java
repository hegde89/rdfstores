package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.search.datastructure.IResultPage;
import edu.unika.aifb.graphindex.data.Table;

/**
 * 
 * @author andi
 */
public class ResultPage extends Result implements IResultPage {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4867269762506495652L;

	/*
	 * 
	 */
	public static final ResultPage EMPTY_PAGE = new ResultPage();

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(ResultPage.class);

	/*
	 * 
	 */
	private int m_page;

	public ResultPage() {
		super();
	}

	public ResultPage(Table<String> resultTable, int pageNum) {
		super(resultTable);
		m_page = pageNum;
	}

	public int getPageNum() {
		return m_page;
	}

	public void setPageNum(int pageNum) {
		m_page = pageNum;
	}
}
