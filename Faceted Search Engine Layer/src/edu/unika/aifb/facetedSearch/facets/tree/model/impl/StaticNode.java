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
package edu.unika.aifb.facetedSearch.facets.tree.model.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.model.impl.AbstractSingleFacetValue;
import edu.unika.aifb.facetedSearch.facets.tree.model.IStaticNode;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionCache;

/**
 * @author andi
 * 
 */
public class StaticNode extends Node implements IStaticNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5743733958338988213L;
	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StaticNode.class);

	/*
	 * 
	 */
	private SearchSession m_session;

	private SearchSessionCache m_cache;

	/*
	 * 
	 */
	protected int m_countFV;

	protected int m_countS;
	private int m_height;
	private int m_depth;
	private int m_size;
	
	private boolean m_isTypeLeave;
	
	public StaticNode() {
		super();
		init();
	}

	public StaticNode(String value) {
		super(value);
		init();
	}

	public StaticNode(String value, int type, int content) {
		super(value, type, content);
		init();
	}

	public SearchSessionCache getCache() {
		return m_cache;
	}

	public int getCountFV() {

		if (m_countFV == -1) {
			m_countFV = getCache().getCountFV4StaticNode(this);
		}

		return m_countFV;
	}

	public int getCountS() {

		if (m_countS == -1) {
			m_countS = getCache().getCountS4StaticNode(this);
		}

		return m_countS;
	}

	public int getDepth() {
		return m_depth;
	}

	public int getHeight() {
		return m_height;
	}

	public Set<AbstractSingleFacetValue> getObjects() {

		return getCache().getObjects4StaticNode(this);
	}

	public Set<AbstractSingleFacetValue> getObjects(String subject) {

		return getCache().getObjects4StaticNode(this, subject);
	}

	public SearchSession getSession() {
		return m_session;
	}

	public int getSize() {
		return m_size;
	}

	public Set<String> getSources() throws DatabaseException, IOException {

		return getCache().getSources4StaticNode(this);
	}

	public Collection<String> getSubjects() throws DatabaseException,
			IOException {

		return getCache().getSubjects4Node(this);
	}

	private void init() {

		m_isTypeLeave = false;
		m_countFV = -1;
		m_countS = -1;
		m_height = -1;
		m_size = -1;
	}

	public boolean isTypeLeave() {
		return m_isTypeLeave;
	}

	public void setCache(SearchSessionCache cache) {
		m_cache = cache;
	}

	public void setCountFV(int countFV) {
		m_countFV = countFV;
	}

	public void setCountS(int countS) {
		m_countS = countS;
	}

	public void setDepth(int depth) {
		m_depth = depth;
	}

	public void setHeight(int height) {
		m_height = height;
	}

	public void setSession(SearchSession session) {

		m_session = session;
		setCache(session.getCache());
	}

	public void setSize(int size) {
		m_size = size;
	}

	public void setTypeLeave(boolean isTypeLeave) {
		m_isTypeLeave = isTypeLeave;
	}
}
