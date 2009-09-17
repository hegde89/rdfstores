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
package edu.unika.aifb.facetedSearch.index;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.index.builder.impl.FacetIndexHelper;
import edu.unika.aifb.facetedSearch.index.builder.impl.FacetTreeIndexBuilder;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class FacetIndexCreator {

	private IndexDirectory m_idxDirectory;
	// private IndexConfiguration m_idxConfig;

	private final static Logger s_log = Logger
			.getLogger(FacetIndexCreator.class);

	public FacetIndexCreator(IndexDirectory indexDirectory) throws IOException {
		m_idxDirectory = indexDirectory;
		// m_idxConfig = new IndexConfiguration();
	}

	public void create() {

		s_log.debug("start building facet index.");

		try {

			// FacetIndex facetIndex = new FacetIndex(m_idxDirectory,
			// m_idxConfig);
			IndexReader idxReader = new IndexReader(m_idxDirectory);
			FacetIndexHelper helper = FacetIndexHelper.getInstance(idxReader,
					m_idxDirectory);

			// s_log.debug("start building facet index vPos ... ");
			//
			// // Vector Position Index
			// FacetVPosIndexBuilder vPosIndexBuilder = new
			// FacetVPosIndexBuilder(
			// this.m_idxDirectory, idxReader, helper);
			// vPosIndexBuilder.build();
			// vPosIndexBuilder.close();
			//
			// s_log.debug("facet index vPos finished!");

			// helper.setVPosIndex(facetIndex.getVPosIndex());

			// Facet Tree Index

			s_log.debug("start building facet trees ... ");

			FacetTreeIndexBuilder treeBuilder = new FacetTreeIndexBuilder(
					this.m_idxDirectory, idxReader, helper);
			treeBuilder.build();
			treeBuilder.close();

			// helper.setLeaveDB(facetIndex.getDbIndex(FacetIndexName.LEAVE));
			// helper.setEndPointDB(facetIndex
			// .getDbIndex(FacetIndexName.ENDPOINTS));
			// helper.setPropertyEndPointDB(facetIndex.getEndPointDB());
			// helper.setLiteralDB(facetIndex.getLiteralDB());

			s_log.debug("facet trees finished!");

			// s_log.debug("start object index ... ");
			//
			// FacetObjectIndexBuilder objectBuilder = new
			// FacetObjectIndexBuilder(
			// this.m_idxDirectory, idxReader, helper);
			//			
			// objectBuilder.build();
			// objectBuilder.close();
			//			
			// s_log.debug("object index finished!");

			// s_log.debug("start building literal distance index ... ");
			//
			// // Distance Index
			// FacetDistanceBuilder distanceBuilder = new FacetDistanceBuilder(
			// this.m_idxDirectory, helper, idxReader, false);
			//
			// distanceBuilder.build();
			// distanceBuilder.close();
			//
			// s_log.debug("literal distance index trees finished!");

			FacetIndexHelper.close();

		} catch (EnvironmentLockedException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}

		s_log.debug("facet index finished!");
	}
}
