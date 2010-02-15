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
package edu.unika.aifb.facetedSearch.search.session.cache;

import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;

/**
 * @author andi
 * 
 */
public class SearchSessionCacheManager {

	private static SearchSessionCacheManager s_instance;

	public static SearchSessionCacheManager getInstance() {
		return s_instance == null
				? s_instance = new SearchSessionCacheManager()
				: s_instance;
	}

	private SearchSessionCache[] m_caches;

	private SearchSessionCacheManager() {
		m_caches = new SearchSessionCache[FacetedSearchLayerConfig
				.getMaxSearchSessions()];
	}

	public SearchSessionCache get(int sessionID) {
		return m_caches[sessionID];
	}

	public void put(int sessionID, SearchSessionCache cache) {
		m_caches[sessionID] = cache;
	}
}