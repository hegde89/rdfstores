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
package edu.unika.aifb.facetedSearch.search.session;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.search.session.SearchSession.SessionStatus;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionFactory.SearchSessionFactoryUtils;

/**
 * @author andi
 * 
 */
public class SearchSessionCleaningThread extends Thread {

	private static Logger s_log = Logger
			.getLogger(SearchSessionCleaningThread.class);

	private long m_sleep;

	public SearchSessionCleaningThread() {

		super(FacetEnvironment.DefaultValue.CLEANER_NAME);
		m_sleep = FacetedSearchLayerConfig.getCleaningInterval();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {

		while (!isInterrupted()) {

			for (int i = 0; i < FacetedSearchLayerConfig.getMaxSearchSessions(); i++) {

				if (SearchSessionFactoryUtils.isExpired(i)) {

					if (SearchSessionFactory.getPool()[i].isFree()) {

						SearchSessionFactory.getLocks()[i] = 0;
						SearchSessionFactory.getPool()[i].clean();
						SearchSessionFactory.getPool()[i]
								.setHttpSessionId(FacetEnvironment.DefaultValue.CLEANER_ID);
						SearchSessionFactory.getPool()[i]
								.setStatus(SessionStatus.FREE);
						SearchSessionFactory.getPool()[i].touch();

						s_log.debug("cleaned search session '" + i + "'");
					}
				}
			}

			try {
				Thread.sleep(m_sleep);
			} catch (InterruptedException e) {
				interrupt();
			}
		}
	}
}