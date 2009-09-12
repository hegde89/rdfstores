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
package edu.unika.aifb.facetedSearch.store.impl;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.exception.ExceptionHelper;
import edu.unika.aifb.facetedSearch.index.FacetIndex;
import edu.unika.aifb.facetedSearch.index.FacetIndexCreator;
import edu.unika.aifb.facetedSearch.search.evaluator.GenericQueryEvaluator;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.IStore;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.index.Index;
import edu.unika.aifb.graphindex.index.IndexConfiguration;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class GenericRdfStore implements IStore {

	public enum IndexName {
		FACET_INDEX, STRUCTURE_INDEX
	}

	private SearchSession m_session;

	// IndexReader & IndexDir
	private IndexReader m_idxReader;
	private IndexDirectory m_idxDir;
	// Indices
	private FacetIndex m_facetIndex;
	private StructureIndex m_structureIndex;

	public GenericRdfStore(Properties props, String action) throws IOException,
			StorageException, InterruptedException {

		if (action.equals(FacetEnvironment.CREATE_STORE)) {
			this.createStore(props);
		} else {
			this.loadStore(props.getProperty(FacetEnvironment.INDEX_DIRECTORY));
		}
	}

	private void createStore(Properties props) throws IOException,
			StorageException, InterruptedException {

		String files = props.getProperty(FacetEnvironment.FILES);
		List<String> file_list = new ArrayList<String>();

		// check if file is a directory, if yes, import all files in the
		// directory
		File f = new File(files);
		if (f.isDirectory()) {
			for (File file : f.listFiles()) {
				if (!file.getName().startsWith(".")) {
					file_list.add(file.getAbsolutePath());
				}
			}
		} else {
			file_list.add(files);
		}

		/*
		 * Note, all files must have same type. TODO: allow files to have
		 * different types.
		 */

		Importer importer;
		if (props.getProperty(FacetEnvironment.ONTO_LANGUAGE).equals(
				FacetEnvironment.OntologyLanguage.N_3)) {
			importer = new NxImporter();
		} else if (props.getProperty(FacetEnvironment.ONTO_LANGUAGE).equals(
				FacetEnvironment.OntologyLanguage.RDF)) {
			importer = new RDFImporter();
		} else {
			throw new InvalidParameterException(ExceptionHelper.createMessage(
					FacetEnvironment.FILES, ExceptionHelper.Cause.NOT_VALID));
		}

		importer.setIgnoreDataTypes(false);
		importer.addImports(file_list);

		IndexCreator ic = new IndexCreator(this.m_idxDir = new IndexDirectory(
				props.getProperty(FacetEnvironment.INDEX_DIRECTORY)));

		importer.setIgnoreDataTypes(new Boolean(props
				.getProperty(FacetEnvironment.IGNORE_DATATYPES)));

		// the importer is the data source
		ic.setImporter(importer);

		// create a data index (default: true)
		ic.setCreateDataIndex(new Boolean(props
				.getProperty(FacetEnvironment.CREATE_DATA_INDEX)));

		// create structure index (default: true)
		ic.setCreateStructureIndex(new Boolean(props
				.getProperty(FacetEnvironment.CREATE_STRUCTURE_INDEX)));

		// create keyword index (default: true)
		ic.setCreateKeywordIndex(new Boolean(props
				.getProperty(FacetEnvironment.CREATE_KEYWORD_INDEX)));

		// set neighborhood size to 2 (default: 0)
		ic.setKWNeighborhoodSize(Integer.parseInt(props
				.getProperty(FacetEnvironment.NEIGHBORHOOD_SIZE)));

		// set structure index path length to 1 (default: 1)
		ic.setSIPathLength(Integer.parseInt(props
				.getProperty(FacetEnvironment.STRUCTURE_INDEX_PATH_LENGTH)));

		// include data values in structure index (not graph) (default: true)
		ic
				.setStructureBasedDataPartitioning(new Boolean(
						props
								.getProperty(FacetEnvironment.STRUCTURE_BASED_DATA_PARTIONING)));

		ic.setSICreateDataExtensions(new Boolean(props
				.getProperty(FacetEnvironment.CREATE_DATA_EXTENSIONS)));

		// create index
		ic.create();

		// create facet indices
		if (Boolean.getBoolean(props
				.getProperty(FacetEnvironment.FACETS_ENABLED))) {

			FacetIndexCreator fic = new FacetIndexCreator(m_idxDir);
			fic.create();
		}

		this.m_idxReader = new IndexReader(this.m_idxDir);
	}

	public GenericQueryEvaluator getEvaluator() {
		return new GenericQueryEvaluator(m_session, m_idxReader);
	}

	/**
	 * @return the idxDir
	 */
	public IndexDirectory getIdxDir() {
		return m_idxDir;
	}

	/**
	 * @return the idxReader
	 */
	public IndexReader getIdxReader() {
		return m_idxReader;
	}

	public Index getIndex(IndexName idxName) throws EnvironmentLockedException,
			DatabaseException, IOException, StorageException {

		switch (idxName) {

		case FACET_INDEX: {

			if (m_facetIndex == null) {
				m_facetIndex = new FacetIndex(m_idxDir,
						new IndexConfiguration());
			}
			return m_facetIndex;
		}
		case STRUCTURE_INDEX: {

			if (m_structureIndex == null) {
				m_structureIndex = new StructureIndex(m_idxReader);
			}
			return m_facetIndex;
		}

		default: {
			return null;
		}
		}

	}

	// public Map<String, String> getObjects(String subject) {
	// // TODO
	// return null;
	// }
	//
	// // /**
	// // * @return the session
	// // */
	// // public SearchSession getSession() {
	// // return m_session;
	// // }
	//
	// public Map<String, String> getSubjects(String object) {
	// // TODO
	// return null;
	// }

	private void loadStore(String dir) throws IOException {
		this.m_idxReader = new IndexReader(this.m_idxDir = new IndexDirectory(
				dir));
	}

	// public void setSession(SearchSession session) {
	// this.m_session = session;
	// }
}