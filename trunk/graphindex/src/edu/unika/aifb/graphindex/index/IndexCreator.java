package edu.unika.aifb.graphindex.index;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.algorithm.largercp.LargeRCP;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;

/**
 * Create an index directory. An index directory consists of multiple indexes
 * and other files.
 * 
 * Indexes:
 * <ul>
 * <li>Data index: the RDF graph stored in a vertical partitioned way using sextuple indexing.</li>
 * <li>Structure index: structure index graph and structure index, created using a bisimulation algorithm
 * with a specified path length.</li>
 * <li>Keyword index (entity index): Index of entities, supporting keyword search on their attributes.</li>
 * </ul>
 * 
 * There are multiple methods to set configuration options, such as which indexes to create and index-specific
 * options. The data index is always created as it's used as a source for the creation of the other indexes.
 * By default all indexes are created, the structure index with a path length of 1 and the keyword index without
 * a neighborhood.
 * 
 * If the specified directory does not exists, it is created. If it does exist and is non-empty, all contents
 * (including previously created indexes) are overwritten.
 * 
 * Use {@link IndexReader} for read-access to an index directory.
 * @author gla
 */
public class IndexCreator implements TripleSink {

	private IndexDirectory m_idxDirectory;
	private IndexConfiguration m_idxConfig;
	private Importer m_importer;
	
	private Map<IndexDescription,IndexStorage> m_dataIndexes;
	
	private Set<String> m_properties;
	
	private final static Logger log = Logger.getLogger(IndexCreator.class);
	
	public IndexCreator(IndexDirectory indexDirectory) throws IOException {
		m_idxDirectory = indexDirectory;
		m_idxConfig = new IndexConfiguration();
		m_properties = new HashSet<String>();
	}
	
	/**
	 * Setting to turn on creation of a structure index.
	 * @param createSI
	 */
	public void setCreateStructureIndex(boolean createSI) {
		m_idxConfig.set(IndexConfiguration.HAS_SP, createSI);
	}

	/**
	 * Setting to turn on creation of the data index. Currently useless,
	 * data index will always be created.
	 * @param createDI
	 */
	public void setCreateDataIndex(boolean createDI) {
		m_idxConfig.set(IndexConfiguration.HAS_DI, createDI);
	}

	/**
	 * Setting to turn on creation of a keyword index.
	 * @param createKW
	 */
	public void setCreateKeywordIndex(boolean createKW) {
		m_idxConfig.set(IndexConfiguration.HAS_KW, createKW);
	}
	
	/**
	 * Sets the path length used for creation of the structure index.
	 * @param pathLength
	 */
	public void setSIPathLength(int pathLength) {
		m_idxConfig.set(IndexConfiguration.SP_PATH_LENGTH, pathLength);
	}
	
	/**
	 * Setting to turn on creation of data extensions. Off by default and is
	 * usually not necessary.
	 * @param createDataExts
	 */
	public void setSICreateDataExtensions(boolean createDataExts) {
		m_idxConfig.set(IndexConfiguration.SP_DATA_EXTENSIONS, createDataExts);
	}
	
	/**
	 * Set to true (the default) the structure index will capture data value,
	 * so that structure index evaluators can evaluate queries. This is different
	 * from data extensions as the data values are ignored for the creation of
	 * the structure index graph, but are indexed in the structure-based index.
	 * @param dpSPBased
	 */
	public void setStructureBasedDataPartitioning(boolean dpSPBased) {
		m_idxConfig.set(IndexConfiguration.DP_SP_BASED, dpSPBased);
	}
	
	/**
	 * Sets the neighborhood size.
	 * @param nsize
	 */
	public void setKWNeighborhoodSize(int nsize) {
		m_idxConfig.set(IndexConfiguration.KW_NSIZE, nsize);
	}
	
	public void setImporter(Importer importer) {
		m_importer = importer;
	}
	
	private void addDataIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.DI_INDEXES, idx);
	}
	
	private void addSPIndex(IndexDescription idx) {
		m_idxConfig.addIndex(IndexConfiguration.SP_INDEXES, idx);
	}
	
	/**
	 * Creates indexes. Option have to be set beforehand.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws StorageException
	 * @throws EnvironmentLockedException
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	public void create() throws FileNotFoundException, IOException, StorageException, InterruptedException {
		m_idxDirectory.create();

		addDataIndex(IndexDescription.SCOP);
		addDataIndex(IndexDescription.OCPS);
		addDataIndex(IndexDescription.PSOC);
		addDataIndex(IndexDescription.CPSO);
		addDataIndex(IndexDescription.POCS);
		addDataIndex(IndexDescription.SOPC);
		
		addSPIndex(IndexDescription.PSESO);
		addSPIndex(IndexDescription.POESS);
		addSPIndex(IndexDescription.SES);
		addSPIndex(IndexDescription.POES);
		
		importData();
		
		analyzeData();

		if (m_idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
			log.debug("creating structure index");
			try {
				index();
				createSPIndexes();
			} catch (EnvironmentLockedException e) {
				throw new StorageException(e);
			} catch (DatabaseException e) {
				throw new StorageException(e);
			}
		}
		else
			log.debug("not creating structure index");
		
		if (m_idxConfig.getBoolean(IndexConfiguration.HAS_KW)) {
			log.debug("creating keyword index");
			createKWIndex();
		}
		else
			log.debug("not creating keyword index");

		m_idxConfig.store(m_idxDirectory);
	}
	
	private void importData() throws IOException, StorageException {
		m_importer.setTripleSink(this);
		
		m_dataIndexes = new HashMap<IndexDescription,IndexStorage>();
		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			IndexStorage is = new LuceneIndexStorage(new File(m_idxDirectory.getDirectory(IndexDirectory.VP_DIR, true).getAbsolutePath() + "/" + idx.getIndexFieldName()));
			is.initialize(true, false);
			m_dataIndexes.put(idx, is);
		}
		
		m_importer.doImport();
		
		Util.writeEdgeSet(m_idxDirectory.getFile(IndexDirectory.PROPERTIES_FILE, true), m_properties);

		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			log.debug("merging " + idx.toString());
			m_dataIndexes.get(idx).mergeIndex(idx);
			m_dataIndexes.get(idx).close();
		}
	}
	
	private void analyzeData() throws StorageException, IOException {
		DataIndex dataIndex = new DataIndex(m_idxDirectory, m_idxConfig);

		Set<String> objectProperties = new HashSet<String>();
		Set<String> dataProperties = new HashSet<String>();

		for (String property : Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.PROPERTIES_FILE))) {
			if (hasEntity(dataIndex, property))
				objectProperties.add(property);
			else
				dataProperties.add(property);
		}
		
		Util.writeEdgeSet(m_idxDirectory.getFile(IndexDirectory.DATA_PROPERTIES_FILE), dataProperties);
		Util.writeEdgeSet(m_idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE), objectProperties);
		
		log.debug("data properties: " + dataProperties.size() + ", object properties: " + objectProperties.size());
	}
	
	private void index() throws EnvironmentLockedException, DatabaseException, IOException, StorageException, InterruptedException {
		DataIndex dataIndex = new DataIndex(m_idxDirectory, m_idxConfig);
		
		Set<String> edges = new HashSet<String>();
		edges.addAll(Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE)));
		if (m_idxConfig.getBoolean(IndexConfiguration.SP_DATA_EXTENSIONS))
			edges.addAll(Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.DATA_PROPERTIES_FILE)));
		
		int pathLength = m_idxConfig.getInteger(IndexConfiguration.SP_PATH_LENGTH);
		
		log.debug("properties: " + edges.size());
		log.debug("path length: " + pathLength);
		
		EnvironmentConfig config = new EnvironmentConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);

		Environment env = new Environment(m_idxDirectory.getDirectory(IndexDirectory.BDB_DIR, true), config);

		Set<String> backwardEdges = edges;
		Set<String> forwardEdges = m_idxConfig.getBoolean(IndexConfiguration.SP_BACKWARD_ONLY) ? new HashSet<String>() : edges;
		
		LargeRCP rcp = new LargeRCP(dataIndex, env, forwardEdges, backwardEdges);
		rcp.setIgnoreDataValues(true);
		rcp.setTempDir(m_idxDirectory.getDirectory(IndexDirectory.TEMP_DIR, true).getAbsolutePath());

		rcp.createIndexGraph(pathLength);
		
		dataIndex.close();
	}
	
	private void createSPIndexes() throws StorageException, IOException, DatabaseException {
		DataIndex dataIndex = new DataIndex(m_idxDirectory, m_idxConfig);
		IndexStorage is = new LuceneIndexStorage(m_idxDirectory.getDirectory(IndexDirectory.SP_IDX_DIR, true));
		is.initialize(true, false);
		IndexStorage gs = new LuceneIndexStorage(m_idxDirectory.getDirectory(IndexDirectory.SP_GRAPH_DIR, true));
		gs.initialize(true, false);
		BlockCache bc = new BlockCache(m_idxDirectory);
		
		Set<String> objectProperties = Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE));
		Set<String> dataProperties =  Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.DATA_PROPERTIES_FILE));
		
		Set<String> indexEdges = new HashSet<String>();
		int triples = 0;
		for (String property : objectProperties) {
			log.debug("object prop: " + property);
			for (Iterator<String[]> ti = dataIndex.iterator(property); ti.hasNext(); ) {
				String[] triple = ti.next();
				String s = triple[0];
				String o = triple[2];
				
				String subExt = bc.getBlockName(s);
				String objExt = bc.getBlockName(o);
				
				// build index graph
				String indexEdge = new StringBuilder().append(subExt).append("__").append(property).append("__").append(objExt).toString();
				if (indexEdges.add(indexEdge)) {
					gs.addData(IndexDescription.PSO, new String[] { property, subExt }, objExt);
					gs.addData(IndexDescription.POS, new String[] { property, objExt }, subExt);
				}
				
				// add triples to extensions
				is.addData(IndexDescription.PSESO, new String[] { property, s, subExt}, Arrays.asList(o));
				is.addData(IndexDescription.POESS, new String[] { property, o, subExt}, Arrays.asList(s));
				is.addData(IndexDescription.SES, new String[] { s }, subExt);
				is.addData(IndexDescription.POES, new String[] { property, o }, subExt);
				
				triples++;
				
				if (triples % 100000 == 0)
					log.debug("triples: " + triples);
			}
		}
		
		log.debug("index graph edges: " + indexEdges.size());
		
		if (m_idxConfig.getBoolean(IndexConfiguration.DP_SP_BASED)) {
			triples = 0;
			for (String property : dataProperties) {
				log.debug("data prop: " + property);
				for (Iterator<String[]> ti = dataIndex.iterator(property); ti.hasNext(); ) {
					String[] triple = ti.next();
					String s = triple[0];
					String o = triple[2];
					
					String subExt = bc.getBlockName(s);
					
					if (subExt == null)
						continue;
					
					// add triples to extensions
					is.addData(IndexDescription.PSESO, new String[] { property, s, subExt}, Arrays.asList(o));
					is.addData(IndexDescription.POESS, new String[] { property, o, subExt}, Arrays.asList(s));
					is.addData(IndexDescription.POES, new String[] { property, o }, subExt);
					
					triples++;
					
					if (triples % 100000 == 0)
						log.debug("triples: " + triples);
				}
			}
			log.debug("data triples: " + triples);
		}
		
		log.debug("merging indexes...");
		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.SP_INDEXES))
			is.mergeIndex(idx);
		
		gs.mergeIndex(IndexDescription.PSO);
		gs.mergeIndex(IndexDescription.POS);
		
		is.optimize();
		gs.optimize();
		
		dataIndex.close();
		is.close();
		gs.close();
		bc.close();
	}
	
	private void createKWIndex() throws IOException, StorageException {
		prepareKeywordIndex();
		
		KeywordIndexBuilder kb = new KeywordIndexBuilder(m_idxDirectory, m_idxConfig); 
		kb.indexKeywords();
	}
	
	private void prepareKeywordIndex() throws IOException, StorageException {
		DataIndex dataIndex = new DataIndex(m_idxDirectory, m_idxConfig);

		TreeSet<String> conSet = new TreeSet<String>();
		TreeSet<String> relSet = new TreeSet<String>();
		TreeSet<String> attrSet = new TreeSet<String>();
		TreeSet<String> entSet = new TreeSet<String>();
		
		Set<String> properties = new HashSet<String>();
		properties.addAll(Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.DATA_PROPERTIES_FILE)));
		properties.addAll(Util.readEdgeSet(m_idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE)));

		File concepts = m_idxDirectory.getTempFile("concepts", true);
		File relations = m_idxDirectory.getTempFile("relations", true);
		File entities = m_idxDirectory.getTempFile("entities", true);
		File attributes = m_idxDirectory.getTempFile("attributes", true);
		
		int triples = 0;

		try {
			for (String property : properties) {
				for (Iterator<String[]> i = dataIndex.iterator(property); i.hasNext(); ) {
					String[] t = i.next();
					String s = t[0];
					String p = t[1];
					String o = t[2];

					entSet.add(s);

					if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.RELATION)) {
						relSet.add(p);
						entSet.add(o);
					} else if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.LITERAL)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.ATTRIBUTE)) {
						attrSet.add(p);
					} else if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
							&& TypeUtil.getObjectType(p, o).equals(TypeUtil.CONCEPT)
							&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.TYPE)) {
						conSet.add(o);
					}
					
					triples++;
					if (triples % 1000000 == 0)
						System.out.println(triples);
					
					if (entSet.size() > 1000000) {
						Util.mergeRowSet(entities, entSet);
						entSet.clear();
					}
					
				}
			}
			
			Util.mergeRowSet(entities, entSet);
			Util.writeEdgeSet(concepts, conSet);
			Util.writeEdgeSet(relations, relSet);
			Util.writeEdgeSet(attributes, attrSet);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	} 
	
	private boolean hasEntity(DataIndex dataIndex, String property) throws StorageException {
		IndexDescription idx = dataIndex.getSuitableIndex(DataField.PROPERTY);

		Map<DataField,String> valueMap = new HashMap<DataField,String>();
		valueMap.put(DataField.PROPERTY, property);
		
		for (Iterator<String[]> i = dataIndex.getIndexStorage(idx).iterator(idx, new DataField[] { DataField.OBJECT }, property); i.hasNext(); ) {
			String[] row = i.next();
			if (Util.isEntity(row[0]))
				return true;
		}
		return false;
	}
	
	private String selectByField(DataField df, String s, String p, String o, String c) {
		if (df == DataField.SUBJECT)
			return s;
		else if (df == DataField.PROPERTY)
			return p;
		else if (df == DataField.OBJECT)
			return o;
		else if (df == DataField.CONTEXT)
			return c;
		else
			return null;
	}
	
	public void triple(String s, String p, String o, String c) {
		m_properties.add(p);
		
		if (c == null)
			c = "";
		
		for (IndexDescription idx : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES)) {
			String[] indexFields = new String [idx.getIndexFields().size()];
			
			for (int i = 0; i < indexFields.length; i++) {
				DataField df = idx.getIndexFields().get(i);
				indexFields[i] = selectByField(df, s, p, o, c);

				if (indexFields[i] == null) {
					throw new UnsupportedOperationException("data indexes can only consist of S, P and O data fields");
				}
			}
			
			String value = selectByField(idx.getValueField(), s, p, o, c);
			
			if (value == null) {
				throw new UnsupportedOperationException("data indexes can only consist of S, P and O data fields");
			}
			
			try {
				m_dataIndexes.get(idx).addData(idx, indexFields, value);
			} catch (StorageException e) {
				e.printStackTrace();
			}
		}
	}
}
