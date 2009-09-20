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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.facetedSearch.facets.tree.model.IStaticNode;
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
	private static Logger s_log = Logger.getLogger(StaticNode.class);

	private SearchSessionCache m_cache;

	private HashMap<String, HashSet<Integer>> m_LiteralSources;
	private List<String> m_sortedLiterals;

	private int m_countFV;
	private int m_countS;
	private String m_name;
	private int m_height;
	private int m_depth;
	private int m_size;

	public StaticNode() {
		super();
		init();
	}

	public StaticNode(String value, NodeContent content) {
		super(value, content);
		init();
	}

	public StaticNode(String value, NodeType type) {
		super(value, type);
		init();
	}

	public StaticNode(String value, NodeType type, NodeContent content) {
		super(value, type, content);
		init();
	}

	public void addSortedObjects(HashSet<String> objects, String source) {

		for (String object : objects) {

			if (!m_LiteralSources.containsKey(object)) {

				m_sortedLiterals.add(object);

				HashSet<Integer> sources = new HashSet<Integer>();
				sources.add(source.hashCode());
				m_LiteralSources.put(object, sources);

			} else {

				HashSet<Integer> sources = m_LiteralSources.get(object);
				sources.add(source.hashCode());

				m_LiteralSources.put(object, sources);
			}

			incrementCountFV(1);
		}
	}

	public void addSourceIndivdiual(String ind) {

		if ((m_cache != null)) {

			// && m_cache.isOpen()

			try {
				m_cache.addSourceIndivdual(ind, getID());
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

		} else {

			s_log.debug("cache is not set or not open ... ");
		}
	}

	public void addUnsortedObjects(HashSet<String> objects) {

		if ((m_cache != null)) {

			// && m_cache.isOpen()

			try {

				int objectsInserted = m_cache.addObjects(objects, this);
				incrementCountFV(objectsInserted);

			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}

		} else {

			s_log.debug("cache is not set or not open ... ");
		}
	}

	public int getCountFV() {
		return m_countFV;
	}

	public int getCountS() {
		return m_countS;
	}

	public int getDepth() {
		return m_depth;
	}

	public int getHeight() {
		return m_height;
	}

	public HashMap<String, HashSet<Integer>> getLiteralSources() {
		return m_LiteralSources;
	}

	public HashSet<Integer> getLiteralSources(String lit) {
		return m_LiteralSources.containsKey(lit) ? m_LiteralSources.get(lit)
				: null;
	}

	public String getName() {
		return m_name;
	}

	public HashSet<String> getObjects() throws DatabaseException, IOException {

		if ((m_cache != null)) {

			// && m_cache.isOpen()

			return m_cache.getObjects(getID());

		} else {

			s_log.debug("cache is not set or not open ... ");
			return null;
		}
	}

	public int getSize() {
		return this.m_size;
	}

	public List<String> getSortedLiterals() {
		return m_sortedLiterals;
	}

	public HashSet<String> getSourceIndivdiuals() throws DatabaseException,
			IOException {

		if ((m_cache != null)) {

			// && m_cache.isOpen()

			return m_cache.getSources(getID());

		} else {

			s_log.debug("cache is not set or not open ... ");
			return null;
		}
	}

	public void incrementCountFV(int increment) {
		m_countFV = m_countFV + increment;
	}

	public void incrementCountS(int increment) {
		m_countS = m_countS + increment;
	}

	private void init() {

		m_LiteralSources = new HashMap<String, HashSet<Integer>>();
		m_sortedLiterals = new ArrayList<String>();

		m_countFV = 0;
		m_countS = 0;
		m_height = 0;
		m_size = 0;

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

	public void setLiteralCounts(HashMap<String, HashSet<Integer>> literalCounts) {
		m_LiteralSources = literalCounts;
	}

	public void setName(String name) {
		m_name = name;
	}

	public void setSize(int size) {
		m_size = size;
	}

	public void setSortedLiterals(List<String> sortedLiterals) {
		m_sortedLiterals = sortedLiterals;
	}
}
