package edu.unika.aifb.graphindex.index;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.ho.yaml.Yaml;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.searcher.keyword.analyzer.CapitalizationSplitterAnalyzer;
import edu.unika.aifb.graphindex.searcher.keyword.analyzer.DictionaryCompoundWordAnalyzer;
import edu.unika.aifb.graphindex.searcher.keyword.analyzer.HyphenationCompoundWordAnalyzer;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.keyword.BloomFilter;
import edu.unika.aifb.graphindex.storage.lucene.LuceneNeighborhoodStorage;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;

public class KeywordIndexBuilder {
	
	private static final float CONCEPT_BOOST = 2.0f;
	private static final float RELATION_BOOST = 1.0f;
	private static final float ATTRIBUTE_BOOST = 1.0f;
	private static final float ENTITY_BOOST = 1.0f;
	private static final float ENTITY_DISCRIMINATIVE_BOOST = 5.0f;
	private static final float ENTITY_DESCRIPTIVE_BOOST = 1.0f;
	
	private static int HOP;  
	private static int MAXFIELDLENGTH = 100;
	
	private static double FALSE_POSITIVE = 0.001;

	private IndexDirectory idxDirectory;
	private IndexConfiguration idxConfig;
	protected DataIndex dataIndex;
	private BlockCache  blockSearcher;
	private NeighborhoodStorage ns;
	private Set<String> objectProperties;
	private Set<String> relations;
	private Set<String> attributes;
	private boolean resume = false;
	private Set<String> properties;
	private IndexWriter valueWriter;
//	private Map<String,Double> propertyWeights;
	
	private static final Logger log = Logger.getLogger(KeywordIndexBuilder.class);
	
	public KeywordIndexBuilder(edu.unika.aifb.graphindex.index.IndexReader idxReader, boolean resume) throws IOException, StorageException {
		this.idxDirectory = idxReader.getIndexDirectory();
		this.idxConfig = idxReader.getIndexConfiguration();
		this.dataIndex = idxReader.getDataIndex();
		this.resume = resume;
		
		this.ns = new LuceneNeighborhoodStorage(idxDirectory.getDirectory(IndexDirectory.NEIGHBORHOOD_DIR, !resume));
		this.ns.initialize(!resume, false);
		
//		this.propertyWeights = (Map<String,Double>)Yaml.load(idxDirectory.getFile(IndexDirectory.PROPERTY_FREQ_FILE));
//		log.debug(propertyWeights);
		
		log.info("resume: " + resume);
		
		HOP = idxConfig.getInteger(IndexConfiguration.KW_NSIZE);
		
		if (idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
			try {
				this.blockSearcher = new BlockCache(idxDirectory);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
		
	}

	public void indexKeywords() throws StorageException, IOException {
		File indexDir = idxDirectory.getDirectory(IndexDirectory.KEYWORD_DIR, !resume);
		File valueDir = idxDirectory.getDirectory(IndexDirectory.VALUE_DIR, !resume);

		this.objectProperties = Util.readEdgeSet(idxDirectory.getFile(IndexDirectory.OBJECT_PROPERTIES_FILE));
		this.relations = Util.readEdgeSet(idxDirectory.getTempFile("relations", false));
		this.attributes = Util.readEdgeSet(idxDirectory.getTempFile("attributes", false));
		properties = new HashSet<String>();
		properties.addAll(relations);
		properties.addAll(attributes);
		
		log.debug("attributes: " + attributes.size() + ", relations: " + relations.size());

		try {
//			HyphenationCompoundWordAnalyzer analyzer = new HyphenationCompoundWordAnalyzer("./res/en_hyph_US.xml", "./res/en_US.dic");
//			DictionaryCompoundWordAnalyzer analyzer = new DictionaryCompoundWordAnalyzer("./res/en_US.dic");
			CapitalizationSplitterAnalyzer analyzer = new CapitalizationSplitterAnalyzer();
			StandardAnalyzer valueAnalyzer = new StandardAnalyzer();
			IndexWriter indexWriter = new IndexWriter(indexDir, analyzer, !resume, new MaxFieldLength(MAXFIELDLENGTH));
			log.debug("max terms per field: " + indexWriter.getMaxFieldLength());
			
			valueWriter = new IndexWriter(valueDir, valueAnalyzer, !resume, new MaxFieldLength(MAXFIELDLENGTH));
			
			org.apache.lucene.index.IndexReader reader = null;
			if (resume) {
				reader = org.apache.lucene.index.IndexReader.open(FSDirectory.getDirectory(indexDir), true);
				log.debug("docs: " + reader.numDocs());
			}
			
			if (!resume) {
				log.info("Indexing concepts");
				indexSchema(indexWriter, idxDirectory.getTempFile("concepts", false), TypeUtil.CONCEPT, CONCEPT_BOOST);
				
				log.info("Indexing attributes");
				indexSchema(indexWriter, idxDirectory.getTempFile("attributes", false), TypeUtil.ATTRIBUTE, ATTRIBUTE_BOOST);
				
				log.info("Indexing relations");
				indexSchema(indexWriter, idxDirectory.getTempFile("relations", false), TypeUtil.RELATION, RELATION_BOOST);	
			}
			
			log.info("Indexing entities");
			indexEntity(indexWriter, idxDirectory.getTempFile("entities", false), reader);
			
			indexWriter.commit();
			valueWriter.commit();
			
			log.debug("optimizing...");
			indexWriter.optimize();
			valueWriter.optimize();
			
			indexWriter.close();
			valueWriter.close();
			
			if (blockSearcher != null)
				blockSearcher.close();
			ns.optimize();
			ns.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected  void indexSchema(IndexWriter indexWriter, File file, String type, float boost) throws StorageException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			while ((line = br.readLine()) != null) {
				String tokens [] = line.split("\t");
				String uri,localName;
				if(tokens.length == 1) {
					uri = line.trim();
					localName = TypeUtil.getLocalName(uri).trim(); 
				}
				else {
					continue;
				}
				
				/* Write Index */
				Document doc = new Document();
				
				// indexing type of the element
				doc.add(new Field(Constant.TYPE_FIELD, type, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
				
				// indexing local name
				doc.add(new Field(Constant.SCHEMA_FIELD, localName, Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
				
				// indexing label 
				Table<String> table = dataIndex.getTriples(uri, RDFS.LABEL.stringValue(), null);
				for (String[] row : table) {
					doc.add(new Field(Constant.SCHEMA_FIELD, row[2], Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
				}
				
				// indexing uri
				doc.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
				
				// indexing extension id for concept
				if(type.equals(TypeUtil.CONCEPT) && idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
					String blockName = blockSearcher.getBlockName(uri);
					doc.add(new Field(Constant.EXTENSION_FIELD, blockName, Field.Store.YES, Field.Index.NO));
				}
				doc.setBoost(boost);
				indexWriter.addDocument(doc);
			}
			br.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected List<Field> getFieldsForEntity(String uri) throws IOException, StorageException {
		List<Field> fields = new ArrayList<Field>();
		
		// indexing type of the element
		fields.add(new Field(Constant.TYPE_FIELD, TypeUtil.ENTITY, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		
		// indexing uri
		fields.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		
		// indexing extension id 
		if (idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
			String blockName = blockSearcher.getBlockName(uri);
//			String blockName = structureIndex.getExtension(uri);
			if (blockName == null) {
				log.warn("no ext for " + uri);
				return null;
			}
			fields.add(new Field(Constant.EXTENSION_FIELD, blockName, Field.Store.YES, Field.Index.NO));
		}
		
		// indexing concept of the entity element
		Set<String> concepts = computeConcepts(uri);
		if(concepts != null && concepts.size() != 0) {
			for(String concept : concepts) {
				Field field = new Field(Constant.CONCEPT_FIELD, concept, Field.Store.YES, Field.Index.NO);
				fields.add(field);
			}
		}
		
		// indexing entity descriptions in value index
		computeEntityDescriptions(uri);
		
		List<Field> propertyFields = computeProperties(uri);
		fields.addAll(propertyFields);

		// indexing reachable entities
		Set<String> reachableEntities = computeReachableEntities(uri);
//		BloomFilter bf = new BloomFilter(reachableEntities.size(), NUMBER_HASHFUNCTION);
		BloomFilter bf = new BloomFilter(reachableEntities.size(), FALSE_POSITIVE);
		for(String entity : reachableEntities){
			if(entity.startsWith("http://www."))
				entity = entity.substring(11);
			else if(entity.startsWith("http://"))
				entity = entity.substring(7);
			bf.add(entity);
		} 

		ns.addNeighborhoodBloomFilter(uri, bf);

		return fields;
	}
	
	private void indexEntity(IndexWriter indexWriter, File file, IndexReader reader) throws IOException, StorageException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			int entities = 0;
			double time = System.currentTimeMillis();
			while ((line = br.readLine()) != null) {
				String uri = line.trim();
				
				if (reader != null) {
					TermDocs td = reader.termDocs(new Term(Constant.URI_FIELD, uri));
					if (td.next())
						continue;
				}
				
				Document doc = new Document();

				List<Field> fields = getFieldsForEntity(uri);
				
				if (fields == null)
					continue;
				
				for (Field f : fields)
					doc.add(f);
				
				indexWriter.addDocument(doc);
				
//				indexWriter.commit();
				
				entities++;
				if (entities % 100000 == 0) {
					indexWriter.commit();
					valueWriter.commit();
					ns.commit();
					log.debug("entities indexed: " + entities + " avg: " + ((System.currentTimeMillis() - time)/100000.0));
					time = System.currentTimeMillis();
				}
			}
			br.close();
			
			log.debug(entities + " entities indexed");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private Set<String> computeConcepts(String entityUri)  throws IOException, StorageException {
		Set<String> set = new HashSet<String>(); 

		IndexDescription idx = dataIndex.getSuitableIndex(DataField.PROPERTY, DataField.SUBJECT);
		set = dataIndex.getIndexStorage(idx).getDataSet(idx, DataField.OBJECT, idx.createValueArray(DataField.PROPERTY, RDF.TYPE.stringValue(), DataField.SUBJECT, entityUri));

		return set;
	}  
	
	private Set<String> computeLabels(String entityUri) throws IOException, StorageException {
		Set<String> set = new HashSet<String>(); 

		IndexDescription idx = dataIndex.getSuitableIndex(DataField.PROPERTY, DataField.SUBJECT);
		set = dataIndex.getIndexStorage(idx).getDataSet(idx, DataField.OBJECT, idx.createValueArray(DataField.PROPERTY, RDFS.LABEL.stringValue(), DataField.SUBJECT, entityUri));

		return set;
	} 
	
	private void computeEntityDescriptions(String entityUri) throws IOException, StorageException {
		Document doc = new Document();
		doc.add(new Field(Constant.URI_FIELD, entityUri, Field.Store.YES, Field.Index.NO));
		doc.add(new Field(Constant.ATTRIBUTE_FIELD, Constant.ATTRIBUTE_LOCALNAME, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		Field f = new Field(Constant.CONTENT_FIELD, TypeUtil.getLocalName(entityUri).trim(), Field.Store.NO, Field.Index.ANALYZED);
//		f.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
		doc.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
		valueWriter.addDocument(doc);
		
		Table<String> table = dataIndex.getTriples(entityUri, null, null);
		if (table.rowCount() == 0)
			return;
		for (String[] row : table) {
			String predicate = row[1];
			String object = row[2];
		
			if (!properties.contains(predicate) || relations.contains(predicate)) // TODO maybe index relations
				continue;
			
			doc = new Document();
			doc.add(new Field(Constant.URI_FIELD, entityUri, Field.Store.YES, Field.Index.NO));
			doc.add(new Field(Constant.ATTRIBUTE_FIELD, predicate, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
			
			float boost = ENTITY_DESCRIPTIVE_BOOST;
			if (predicate.equals(RDFS.LABEL.toString()))
				boost = ENTITY_DISCRIMINATIVE_BOOST;
			if (TypeUtil.getLocalName(predicate).contains("name"))
				boost = ENTITY_DISCRIMINATIVE_BOOST;
			
//			boost *= propertyWeights.get(predicate);
			doc.setBoost(boost);
			
			if (attributes.contains(predicate)) {
				f = new Field(Constant.CONTENT_FIELD, object, Field.Store.NO, Field.Index.ANALYZED);
//				f.setBoost(boost);
				doc.add(f);
			}
			else if (relations.contains(predicate)) {
				String localname = TypeUtil.getLocalName(object).trim();
				
				f = new Field(Constant.CONTENT_FIELD, localname, Field.Store.NO, Field.Index.ANALYZED);
//				f.setBoost(boost);
				doc.add(f);
			} 
			
			valueWriter.addDocument(doc);
		}
		
	}
	
	private List<Field> computeProperties(String uri) throws StorageException {
		Set<String> inProperties = new HashSet<String>(), outProperties = new HashSet<String>();
		
		Table<String> table = dataIndex.getTriples(uri, null, null);
		if (table != null && table.rowCount() > 0) {
			for (String[] row : table) {
				if (objectProperties.contains(row[1]))
					outProperties.add(row[1]);
			}
		}

		table = dataIndex.getTriples(null, null, uri);
		if (table != null && table.rowCount() > 0) {
			for (String[] row : table) {
				if (objectProperties.contains(row[1]))
					inProperties.add(row[1]);
			}
		}
		
		// most entities have type anyway
		outProperties.remove(RDF.TYPE.toString());
		
		List<Field> fields = new ArrayList<Field>();
		
		StringBuilder sb = new StringBuilder();
		for (String property : inProperties)
			sb.append(property).append('\n');
		fields.add(new Field(Constant.IN_PROPERTIES_FIELD, sb.toString(), Field.Store.COMPRESS, Field.Index.NO));

		sb = new StringBuilder();
		for (String property : outProperties)
			sb.append(property).append('\n');
		fields.add(new Field(Constant.OUT_PROPERTIES_FIELD, sb.toString(), Field.Store.COMPRESS, Field.Index.NO));

		return fields;
	}
	
	private Set<String> computeNeighbors(String entityUri) throws IOException, StorageException {
		HashSet<String> set = new HashSet<String>(500); 

		Table<String> table = dataIndex.getTriples(entityUri, null, null);

		for (String[] row : table) {
			String predicate = row[1];
			String object = row[2];
			if (relations.contains(predicate)) {
				set.add(object);
			}
		}
		
		table = dataIndex.getTriples(null, null, entityUri);
		for (String[] row : table) {
			String predicate = row[1];
			String subject = row[0];
			if (relations.contains(predicate))
				set.add(subject);
		}

		return set;
	} 
	
	private Set<String> computeReachableEntities(String entityUri) throws IOException, StorageException {
		HashSet<String> reachableEntities = new HashSet<String>(); 
		HashSet<String> currentLayer = new HashSet<String>();
		HashSet<String> nextLayer = new HashSet<String>();
		currentLayer.add(entityUri);
		
		int i = 0;
		while(i < HOP && !currentLayer.isEmpty()) {
			for(String entity : currentLayer) {
				Set<String> neighbors = computeNeighbors(entity);
				if(neighbors != null && neighbors.size() != 0) {
					for(String neighbor : neighbors) {
						if(!reachableEntities.contains(neighbor) && !neighbor.equals(entityUri)) {
							reachableEntities.add(neighbor);
							nextLayer.add(neighbor);
						}
					}
				}	
			}
			currentLayer = nextLayer;
			nextLayer = new HashSet<String>();
			i++;
		}
		reachableEntities.add(entityUri);
		return reachableEntities;
	} 

}
