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
import java.util.Map;
import java.util.Properties;

import org.apexlab.service.session.datastructure.ResultPage;

import edu.unika.aifb.facetedSearch.Environment;
import edu.unika.aifb.facetedSearch.api.model.IAbstractObject;
import edu.unika.aifb.facetedSearch.api.model.IIndividual;
import edu.unika.aifb.facetedSearch.converter.hermes2fsl.QueryConverter;
import edu.unika.aifb.facetedSearch.exception.ExceptionHelper;
import edu.unika.aifb.facetedSearch.exception.MissingParameterException;
import edu.unika.aifb.facetedSearch.index.FacetIndexCreator;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.store.IStore;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.NxImporter;
import edu.unika.aifb.graphindex.importer.RDFImporter;
import edu.unika.aifb.graphindex.index.IndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.Query;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.keyword.ExploringKeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.CombinedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

/**
 * @author andi
 * 
 */
public class GenericRdfStore implements IStore {

	public class GenericQueryEvaluator {

		private GenericQueryEvaluator() {

		}

		public ResultPage evaluate(
				org.apexlab.service.session.datastructure.Query hermesQuery) {

			Query graphIndexQuery = QueryConverter.convert(hermesQuery);

			ResultPage resultPage = null;

			if (graphIndexQuery instanceof KeywordQuery) {

				KeywordQueryEvaluator eval = null;
				Table<String> resultTable;

				try {
					eval = (KeywordQueryEvaluator) GenericRdfStore.this
							.getEvaluator(Environment.EvaluatorType.KeywordQueryEvaluator);
					resultTable = eval.evaluate((KeywordQuery) graphIndexQuery);
					GenericRdfStore.this.m_session.getConstructionDelegator()
							.doFacetConstruction(resultTable);
					resultPage = EvaluatorHelper
							.constructResultPage(resultTable);

				} catch (InvalidParameterException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (StorageException e) {
					e.printStackTrace();
				}

			} else {

				// TODO

			}

			return resultPage;
		}
	}

	private SearchSession m_session;
	private IndexReader m_idxReader;

	private IndexDirectory m_idxDir;

	public GenericRdfStore(Properties props, String action) throws MissingParameterException,
			InvalidParameterException, IOException, StorageException,
			InterruptedException {

		if (action.equals(Environment.CREATE_STORE)) {
			this.createStore(props);
		} else {
			this.loadStore(props.getProperty(Environment.INDEX_DIRECTORY));
		}
	}

	private void createStore(Properties props)
			throws MissingParameterException, InvalidParameterException,
			IOException, StorageException, InterruptedException {

		String files = props.getProperty(Environment.FILES);
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
		if (props.getProperty(Environment.ONTO_LANGUAGE).equals(
				Environment.OntologyLanguage.N_3)) {
			importer = new NxImporter();
		} else if (props.getProperty(Environment.ONTO_LANGUAGE).equals(
				Environment.OntologyLanguage.RDF)) {
			importer = new RDFImporter();
		} else {
			throw new InvalidParameterException(ExceptionHelper.createMessage(
					Environment.FILES, ExceptionHelper.Cause.NOT_VALID));
		}

		importer.setIgnoreDataTypes(false);
		importer.addImports(file_list);

		IndexCreator ic = new IndexCreator(this.m_idxDir = new IndexDirectory(
				props.getProperty(Environment.INDEX_DIRECTORY)));

		// the importer is the data source
		ic.setImporter(importer);

		// create a data index (default: true)
		ic.setCreateDataIndex(new Boolean(props
				.getProperty(Environment.CREATE_DATA_INDEX)));

		// create structure index (default: true)
		ic.setCreateStructureIndex(new Boolean(props
				.getProperty(Environment.CREATE_STRUCTURE_INDEX)));

		// create keyword index (default: true)
		ic.setCreateKeywordIndex(new Boolean(props
				.getProperty(Environment.CREATE_KEYWORD_INDEX)));

		// set neighborhood size to 2 (default: 0)
		ic.setKWNeighborhoodSize(Integer.parseInt(props
				.getProperty(Environment.NEIGHBORHOOD_SIZE)));

		// set structure index path length to 1 (default: 1)
		ic.setSIPathLength(Integer.parseInt(props
				.getProperty(Environment.STRUCTURE_INDEX_PATH_LENGTH)));

		// include data values in structure index (not graph) (default: true)
		ic.setStructureBasedDataPartitioning(new Boolean(props
				.getProperty(Environment.STRUCTURE_BASED_DATA_PARTIONING)));

		ic.setSICreateDataExtensions(true);
		
		// create index
		ic.create();

		// create facet indices
		FacetIndexCreator fic = new FacetIndexCreator(m_idxDir);		
		fic.create();
		
		this.m_idxReader = new IndexReader(this.m_idxDir);
	}

	public GenericQueryEvaluator getEvaluator() {
		return new GenericQueryEvaluator();
	}

	private Searcher getEvaluator(Environment.EvaluatorType type)
			throws IOException, StorageException, InvalidParameterException {

		Searcher searcher = null;

		switch (type) {

		case VPEvaluator: {
			searcher = new VPEvaluator(this.m_idxReader);
			break;
		}
		case CombinedQueryEvaluator: {
			searcher = new CombinedQueryEvaluator(this.m_idxReader);
			break;
		}
		case KeywordQueryEvaluator: {
			searcher = new ExploringKeywordQueryEvaluator(this.m_idxReader);
			break;
		}
		default: {
			throw new InvalidParameterException(ExceptionHelper.createMessage(
					"EvaluatorType", ExceptionHelper.Cause.NOT_VALID));
		}
		}

		return searcher;
	}

	public Map<String, IAbstractObject> getObjects(IAbstractObject subject) {
		// TODO
		return null;
	}

	public Map<String, IIndividual> getSubjects(IAbstractObject object) {
		// TODO
		return null;
	}

	private void loadStore(String dir) throws IOException {
		this.m_idxReader = new IndexReader(this.m_idxDir = new IndexDirectory(
				dir));
	}

	public void setSession(SearchSession session) {
		this.m_session = session;
	}
}