package edu.unika.aifb.keywordsearch.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.storage.BlockStorage;
import edu.unika.aifb.graphindex.storage.DataStorage;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;

public class KeywordIndexBuilder {
	
	private static final float CONCEPT_BOOST = 10.0f;
	private static final float RELATION_BOOST = 5.0f;
	private static final float ATTRIBUTE_BOOST = 5.0f;
	private static final float ENTITY_BOOST = 5.0f;
	private static final float ENTITY_DISCRIMINATIVE_BOOST = 10.0f;
	private static final float ENTITY_DESCRIPTIVE_BOOST = 5.0f;
	
	private static final int HOP = 5;  
	private static final int MAXFIELDLENGTH = 100;
	
	private StructureIndexReader structureIndexReader;
	private StructureIndex index;
	private IndexSearcher dataSearcher;
	private IndexSearcher blockSearcher;
	private String outputDir;
	
	private static final Logger log = Logger.getLogger(KeywordIndexBuilder.class);
	
	public KeywordIndexBuilder(String outputDir) throws StorageException, IOException {
		this.outputDir = outputDir;
		this.structureIndexReader = new StructureIndexReader(outputDir);
		this.index = this.structureIndexReader.getIndex();
		index.getExtensionManager().setMode(ExtensionManager.MODE_READONLY);
		this.dataSearcher = this.index.getDataManager().getDataStorage().getIndexSearcher();
		this.blockSearcher = this.index.getBlockManager().getBlockStorage().getIndexSearcher();
	}
	
	public void indexKeywords() {
		File indexDir = new File(this.outputDir + "/keyword");
		if (!indexDir.exists()) {
			indexDir.mkdirs();
		}
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			IndexWriter indexWriter = new IndexWriter(indexDir, analyzer,true);
			indexWriter.setMaxFieldLength(MAXFIELDLENGTH);
			indexSchema(indexWriter, outputDir + "/concepts", TypeUtil.CONCEPT, CONCEPT_BOOST);
			indexSchema(indexWriter, outputDir + "/attributes", TypeUtil.ATTRIBUTE, ATTRIBUTE_BOOST);
			indexSchema(indexWriter, outputDir + "/relations", TypeUtil.RELATION, RELATION_BOOST);	
			indexEntity(indexWriter, outputDir + "/entities");
			indexWriter.optimize();
			indexWriter.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected  void indexSchema(IndexWriter indexWriter, String file, String type, float boost) {
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
				
				localName = localName.toLowerCase();
				/* Write Index */
				Document doc = new Document();
				
				// indexing type of the schema element
				doc.add(new Field(Constant.TYPE_FIELD, type, Field.Store.YES, Field.Index.UN_TOKENIZED));
				
				// indexing local name
				doc.add(new Field(Constant.SCHEMA_FIELD, localName, Field.Store.YES,Field.Index.TOKENIZED));
				
				// indexing label 
				BooleanQuery bq = new BooleanQuery();
				TermQuery tq = new TermQuery(new Term(DataStorage.SRC_FIELD, uri));
				bq.add(tq, BooleanClause.Occur.MUST);
				tq = new TermQuery(new Term(DataStorage.EDGE_FIELD, RDFS.LABEL.stringValue()));
				bq.add(tq, BooleanClause.Occur.MUST);
				Hits lhits = dataSearcher.search(bq);
				if(lhits != null && lhits.length() != 0) {
					Iterator iter = lhits.iterator();
					while(iter.hasNext()) {
						Document ldoc = ((Hit)iter.next()).getDocument();
						String label = ldoc.get(DataStorage.DST_FIELD);
						doc.add(new Field(Constant.SCHEMA_FIELD, label, Field.Store.YES,Field.Index.TOKENIZED));
					}
				} 
				
				// indexing uri
				doc.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.NO));
				
				// indexing extension id for concept
				if(type.equals(TypeUtil.CONCEPT)){
					TermQuery q = new TermQuery(new Term(BlockStorage.ELE_FIELD, uri));
					Hits hits = blockSearcher.search(q);
					if(hits != null && hits.length() != 0) {
						Document edoc = ((Hit)hits.iterator().next()).getDocument();
						doc.add(new Field(Constant.EXTENSION_FIELD, edoc.get(BlockStorage.BLOCK_FIELD), Field.Store.YES, Field.Index.NO));
					} 
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
	
	public void indexEntity(IndexWriter indexWriter, String file) throws IOException {
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
				
				localName = localName.toLowerCase();
				/* Write Index */
				Document doc = new Document();
				
				// indexing type of the entity element
				doc.add(new Field(Constant.TYPE_FIELD, TypeUtil.ENTITY, Field.Store.YES, Field.Index.NO));
				
				// indexing local name
				Field field = new Field(Constant.LOCALNAME, localName, Field.Store.YES, Field.Index.TOKENIZED);
				field.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
				doc.add(field);
				
				// indexing uri
				doc.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.NO));
				
				// indexing extension id 
				TermQuery q = new TermQuery(new Term(BlockStorage.ELE_FIELD, uri));
				Hits hits = blockSearcher.search(q);
				if (hits != null && hits.length() != 0) {
					Document edoc = ((Hit) hits.iterator().next()).getDocument();
					doc.add(new Field(Constant.EXTENSION_FIELD, edoc.get(BlockStorage.BLOCK_FIELD), Field.Store.YES, Field.Index.NO));
				}
				
				// indexing label
				Set<String> labels = computeLabels(uri);
				if(labels != null && labels.size() != 0)
				for(String label : labels){
					field = new Field(Constant.LABEL, label, Field.Store.YES, Field.Index.TOKENIZED);
					field.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
					doc.add(field);
				} 
				
				// indexing attribute-value compounds
				Set<String> compounds = computeAttributeValueCompounds(uri);
				if(compounds != null && compounds.size() != 0)
				for(String compound : compounds){
					String[] str = compound.trim().split("__");
					String attribute,value;
					if(str.length == 2) {
						attribute = str[1];
						value = str[0]; 
					}
					else {
						continue;
					}
					
					field = new Field(attribute, value, Field.Store.YES,Field.Index.TOKENIZED);
					field.setBoost(ENTITY_DESCRIPTIVE_BOOST);
					doc.add(field);
				} 
				
				// indexing relation-entityID compounds
				compounds = computeRealtionEntityCompounds(uri);
				if(compounds != null && compounds.size() != 0)
				for(String compound : compounds){
					String[] str = compound.trim().split("__");
					String relation,entityId;
					if(str.length == 2) {
						relation = str[1];
						entityId = str[0]; 
					}
					else {
						continue;
					}
					
					field = new Field(relation, entityId, Field.Store.YES,Field.Index.TOKENIZED);
					field.setBoost(ENTITY_DESCRIPTIVE_BOOST);
					doc.add(field);
				} 
				
				// indexing reachable entities
				Set<String> reachableEntities = computeReachableEntities(uri);
				for(String entity : reachableEntities){
					doc.add(new Field(Constant.NEIGHBORHOOD_FIELD, entity, Field.Store.YES,Field.Index.NO));
				} 
				doc.setBoost(ENTITY_BOOST);
				indexWriter.addDocument(doc);
			}
			br.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Set<String> computeRealtionEntityCompounds(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		
		BooleanQuery bqfw = new BooleanQuery();
		TermQuery tqfw = new TermQuery(new Term(DataStorage.SRC_FIELD, entityUri));
		bqfw.add(tqfw, BooleanClause.Occur.MUST);
		tqfw = new TermQuery(new Term(DataStorage.TYPE_FIELD, TypeUtil.RELATION));
		bqfw.add(tqfw, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bqfw);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String relation = doc.get(DataStorage.EDGE_FIELD);
				String entity = doc.get(DataStorage.DST_FIELD);
				
				String localname = TypeUtil.getLocalName(entity).trim();
				set.add(localname + "__" + relation);
				
				Set<String> entitylabels = computeLabels(entity);
				if(entitylabels != null && entitylabels.size() != 0) {
					for(String label : entitylabels) {
						set.add(label + "__" + relation);
					}
				}
			}
			return set;
		} else 
			return null;
	}
	
	public Set<String> computeLabels(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		BooleanQuery bq = new BooleanQuery();
		TermQuery tq = new TermQuery(new Term(DataStorage.SRC_FIELD, entityUri));
		bq.add(tq, BooleanClause.Occur.MUST);
		tq = new TermQuery(new Term(DataStorage.TYPE_FIELD, TypeUtil.LABEL));
		bq.add(tq, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bq);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String attribute = doc.get(DataStorage.EDGE_FIELD);
				String value = doc.get(DataStorage.DST_FIELD);
				if(attribute.equals(RDFS.LABEL.toString()))
					set.add(value);
			}
			return set;
		} else 
			return null;
	} 
	
	public Set<String> computeAttributeValueCompounds(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		BooleanQuery bq = new BooleanQuery();
		TermQuery tq = new TermQuery(new Term(DataStorage.SRC_FIELD, entityUri));
		bq.add(tq, BooleanClause.Occur.MUST);
		tq = new TermQuery(new Term(DataStorage.TYPE_FIELD, TypeUtil.ATTRIBUTE));
		bq.add(tq, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bq);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String attribute = doc.get(DataStorage.EDGE_FIELD);
				String value = doc.get(DataStorage.DST_FIELD);
				set.add(value + "__" + attribute);
			}
			return set;
		} else 
			return null;
	} 
	
	public Set<String> computeNeighbors(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 

		BooleanQuery bqfw = new BooleanQuery();
		TermQuery tqfw = new TermQuery(new Term(DataStorage.SRC_FIELD, entityUri));
		bqfw.add(tqfw, BooleanClause.Occur.MUST);
		tqfw = new TermQuery(new Term(DataStorage.TYPE_FIELD, TypeUtil.RELATION));
		bqfw.add(tqfw, BooleanClause.Occur.MUST);
		
		BooleanQuery bqbw = new BooleanQuery();
		TermQuery tqbw = new TermQuery(new Term(DataStorage.DST_FIELD, entityUri));
		bqbw.add(tqbw, BooleanClause.Occur.MUST);
		tqbw = new TermQuery(new Term(DataStorage.TYPE_FIELD, TypeUtil.RELATION));
		bqbw.add(tqbw, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bqfw);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String entity = doc.get(DataStorage.DST_FIELD);
				set.add(entity);
			}
		} 
		
		hits = dataSearcher.search(bqbw);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String entity = doc.get(DataStorage.SRC_FIELD);
				set.add(entity);
			}
		} 
		
		return set;
	} 
	
	public Set<String> computeReachableEntities(String entityUri) throws IOException {
		HashSet<String> reachableEntities = new HashSet<String>(); 
		LinkedList<String> queue = new LinkedList<String>();
		queue.addFirst(entityUri);
		reachableEntities.add(entityUri);
		
		int i = 0;
		
		while(i <= HOP && !queue.isEmpty()) {
			String entity = queue.getFirst();
			i++;
			Set<String> neighbors = computeNeighbors(entity);
			if(neighbors != null && neighbors.size() != 0)
			for(String neighbor : neighbors) {
				if(!reachableEntities.contains(neighbor)) {
					reachableEntities.add(neighbor);
					queue.addLast(neighbor);
				}
			}
		}
		reachableEntities.remove(entityUri);
		return reachableEntities;
	} 

}
