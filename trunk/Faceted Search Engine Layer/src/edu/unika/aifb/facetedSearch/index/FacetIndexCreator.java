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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.index.builder.impl.FacetIdxBuilderHelper;
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
	private String m_expressivity;
	private File m_dir;

	private final static Logger s_log = Logger
			.getLogger(FacetIndexCreator.class);

	public FacetIndexCreator(IndexDirectory indexDirectory,
			String expressivity, File dir) throws IOException {

		m_dir = dir;
		m_idxDirectory = indexDirectory;
		m_expressivity = expressivity;
	}

	public void create() {

		s_log.debug("start building facet index.");

		try {

			IndexReader idxReader = new IndexReader(m_idxDirectory);
			FacetIdxBuilderHelper helper = FacetIdxBuilderHelper.getInstance(
					idxReader, m_expressivity);

			s_log.debug("start building facet trees ... ");

			FacetTreeIndexBuilder treeBuilder = new FacetTreeIndexBuilder(
					idxReader, helper, m_dir);
			treeBuilder.build();
			treeBuilder.close();

			FacetIdxBuilderHelper.close();

			s_log.debug("facet trees finished!");

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
