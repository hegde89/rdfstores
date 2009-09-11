package edu.unika.aifb.facetedSearch.search.datastructure.impl;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.Table;

/**
 * 
 * @author andi
 */
public class ResultPage extends Result {

	public static final ResultPage EMPTY_PAGE = new ResultPage();

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(ResultPage.class);

	// The number of the current page
	private int m_pageNum;
	private HashMap<String, HashMap<String, ArrayList<String>>> m_facets;

	public ResultPage() {
		super();
	}

	public ResultPage(Table<String> resultTable, int pageNum) {

		// Call to the Result constructor
		super(resultTable);
		m_pageNum = pageNum;
		m_facets = new HashMap<String, HashMap<String, ArrayList<String>>>();

	}

	/**
	 * @return the facets
	 */
	public HashMap<String, HashMap<String, ArrayList<String>>> getFacets() {
		return m_facets;
	}

	/**
	 * @return the pageNum
	 */
	public int getPageNum() {
		return m_pageNum;
	}

	/**
	 * @param facets
	 *            the facets to set
	 */
	public void setFacets(
			HashMap<String, HashMap<String, ArrayList<String>>> facets) {
		m_facets = facets;
	}

	// /**
	// * @param facets
	// * the facets to set
	// */
	// public void setFacets4Domain(String domain,
	// HashMap<String, ArrayList<String>> facets) {
	//
	// if (!m_facets.containsKey(domain)) {
	// m_facets.put(domain, facets);
	// } else {
	// s_log.debug("facets already contained entry for domain '" + domain
	// + "'!");
	// }
	// }

	/**
	 * @param pageNum
	 *            the pageNum to set
	 */
	public void setPageNum(int pageNum) {
		m_pageNum = pageNum;
	}
}
