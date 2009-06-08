package edu.unika.aifb.keywordsearch.index;

//import it.unimi.dsi.util.BloomFilter;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.unika.aifb.graphindex.algorithm.largercp.BlockCache;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;

public class KeywordIndexBuilder {
	
	private static final float CONCEPT_BOOST = 1.0f;
	private static final float RELATION_BOOST = 0.5f;
	private static final float ATTRIBUTE_BOOST = 0.5f;
	private static final float ENTITY_BOOST = 1.0f;
	private static final float ENTITY_DISCRIMINATIVE_BOOST = 1.0f;
	private static final float ENTITY_DESCRIPTIVE_BOOST = 0.5f;
	
	private static final String SEPARATOR = "__";
	
	private static int HOP;  
	private static int MAXFIELDLENGTH = 50;
	
	/*
	 * False positives will happen with probability 2<sup>-<var>NUMBER_HASHFUNCTION</var></sup>
	 * */
	private static int NUMBER_HASHFUNCTION = 5;
	private static double FALSE_POSITIVE = 0.001;
	
	private IndexSearcher dataSearcher;
	private BlockCache  blockSearcher;
	private String outputDir;
	
	private static final Logger log = Logger.getLogger(KeywordIndexBuilder.class);
	
	public KeywordIndexBuilder(String outputDirectory, LuceneGraphStorage gs, Environment env, int hop) {
		this.outputDir = outputDirectory;
		this.dataSearcher = gs.getIndexSearcher();
		HOP = hop;
		try {
			this.blockSearcher = new BlockCache(env);
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};	
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
			log.info("Indexing concepts");
			indexSchema(indexWriter, outputDir + "/temp/concepts", TypeUtil.CONCEPT, CONCEPT_BOOST);
			log.info("Indexing attributes");
			indexSchema(indexWriter, outputDir + "/temp/attributes", TypeUtil.ATTRIBUTE, ATTRIBUTE_BOOST);
			log.info("Indexing relations");
			indexSchema(indexWriter, outputDir + "/temp/relations", TypeUtil.RELATION, RELATION_BOOST);	
			log.info("Indexing entities");
			indexEntity(indexWriter, outputDir + "/temp/entities");
			indexWriter.optimize();
			indexWriter.close();
			
			blockSearcher.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
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
				
				// indexing type of the element
				doc.add(new Field(Constant.TYPE_FIELD, type, Field.Store.YES, Field.Index.UN_TOKENIZED));
				
				// indexing local name
				doc.add(new Field(Constant.SCHEMA_FIELD, localName, Field.Store.YES,Field.Index.TOKENIZED));
				
				// indexing label 
				BooleanQuery bq = new BooleanQuery();
				TermQuery tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_SRC, uri));
				bq.add(tq, BooleanClause.Occur.MUST);
				tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_EDGE, RDFS.LABEL.stringValue()));
				bq.add(tq, BooleanClause.Occur.MUST);
				Hits lhits = dataSearcher.search(bq);
				if(lhits != null && lhits.length() != 0) {
					Iterator iter = lhits.iterator();
					while(iter.hasNext()) {
						Document ldoc = ((Hit)iter.next()).getDocument();
						String label = ldoc.get(LuceneGraphStorage.FIELD_DST);
						doc.add(new Field(Constant.SCHEMA_FIELD, label, Field.Store.YES,Field.Index.TOKENIZED));
					}
				} 
				
				// indexing uri
				doc.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.UN_TOKENIZED));
				
				// indexing extension id for concept
				if(type.equals(TypeUtil.CONCEPT)){
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
	
	public void indexEntity(IndexWriter indexWriter, String file) throws IOException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line;
			int entities = 0;
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
				
//				log.info("Indexing entity " + uri);
				
				localName = localName.toLowerCase();
				/* Write Index */
				Document doc = new Document();
				
				// indexing type of the element
				doc.add(new Field(Constant.TYPE_FIELD, TypeUtil.ENTITY, Field.Store.YES, Field.Index.NO));
				
				// indexing local name
				Field field = new Field(Constant.LOCALNAME_FIELD, localName, Field.Store.YES, Field.Index.TOKENIZED);
				field.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
				doc.add(field);
				
				// indexing uri
				doc.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES, Field.Index.UN_TOKENIZED));
				
				// indexing extension id 
				String blockName = blockSearcher.getBlockName(uri);
				doc.add(new Field(Constant.EXTENSION_FIELD, blockName, Field.Store.YES, Field.Index.NO));
				
				// indexing concept of the entity element
				Set<String> concepts = computeConcepts(uri);
				if(concepts != null && concepts.size() != 0) {
					for(String concept : concepts) {
						field = new Field(Constant.CONCEPT_FIELD, concept, Field.Store.YES, Field.Index.UN_TOKENIZED);
						field.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
						doc.add(field);
					}
				}
				
				// indexing label
				Set<String> labels = computeLabels(uri);
				if(labels != null && labels.size() != 0) {
					for(String label : labels){
						field = new Field(Constant.LABEL_FIELD, label, Field.Store.YES, Field.Index.TOKENIZED);
						field.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
						doc.add(field);
					} 
				}	
				
				// indexing attribute-value and relation-entityID compounds
				Set<String> compounds = computeEntityDescriptions(uri);
				if(compounds != null && compounds.size() != 0) {
					for(String compound : compounds){
						String[] str = compound.trim().split(SEPARATOR);
						String attributeOrRelation,valueOrEntitiyId;
						if(str.length == 2) {
							attributeOrRelation = str[1];
							valueOrEntitiyId = str[0]; 
						}
						else {
							continue;
						}
						
						field = new Field(attributeOrRelation, valueOrEntitiyId, Field.Store.YES, Field.Index.TOKENIZED);
						field.setBoost(ENTITY_DESCRIPTIVE_BOOST);
						doc.add(field);
					} 
				}	
				
				// indexing reachable entities
				Set<String> reachableEntities = computeReachableEntities(uri);
//				BloomFilter bf = new BloomFilter(reachableEntities.size(), NUMBER_HASHFUNCTION);
				BloomFilter bf = new BloomFilter(reachableEntities.size(), FALSE_POSITIVE);
				for(String entity : reachableEntities){
					bf.add(entity);
				} 
				ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
				ObjectOutputStream objectOut = new ObjectOutputStream(byteArrayOut);
				objectOut.writeObject(bf);
				byte[] bytes = byteArrayOut.toByteArray(); 
				doc.add(new Field(Constant.NEIGHBORHOOD_FIELD, bytes, Field.Store.YES));
				doc.setBoost(ENTITY_BOOST);
				indexWriter.addDocument(doc);
				
				entities++;
				if (entities % 1000000 == 0)
					log.debug("entities indexed: " + entities);
			}
			br.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Set<String> computeConcepts(String entityUri)  throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		BooleanQuery bq = new BooleanQuery();
		TermQuery tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_SRC, entityUri));
		bq.add(tq, BooleanClause.Occur.MUST);
		tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_EDGE, RDF.TYPE.stringValue()));
		bq.add(tq, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bq);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String typeEdge = doc.get(LuceneGraphStorage.FIELD_EDGE);
				String concept = doc.get(LuceneGraphStorage.FIELD_DST);
				if(typeEdge.equals(RDF.TYPE.stringValue()))
					set.add(concept);
			}
			return set;
		} else 
			return null;
	}  
	
	public Set<String> computeLabels(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		BooleanQuery bq = new BooleanQuery();
		TermQuery tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_SRC, entityUri));
		bq.add(tq, BooleanClause.Occur.MUST);
		tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_EDGE, RDFS.LABEL.stringValue()));
		bq.add(tq, BooleanClause.Occur.MUST);
		
		Hits hits = dataSearcher.search(bq);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String attribute = doc.get(LuceneGraphStorage.FIELD_EDGE);
				String value = doc.get(LuceneGraphStorage.FIELD_DST);
				if(attribute.equals(RDFS.LABEL.toString()))
					set.add(value);
			}
			return set;
		} else 
			return null;
	} 
	
	private Set<String> computeEntityDescriptions(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 
		
		TermQuery tq = new TermQuery(new Term(LuceneGraphStorage.FIELD_SRC, entityUri));
		Hits hits = dataSearcher.search(tq);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String predicate = doc.get(LuceneGraphStorage.FIELD_EDGE);
				String object = doc.get(LuceneGraphStorage.FIELD_DST);
				
				String type = TypeUtil.checkType(predicate, object);
				if(type.equals(TypeUtil.ATTRIBUTE)) {
					set.add(object + SEPARATOR + predicate);
				}
				else if(type.equals(TypeUtil.RELATION)) {
					String localname = TypeUtil.getLocalName(object).trim();
					set.add(localname + SEPARATOR + predicate);
					
					Set<String> entitylabels = computeLabels(object);
					if(entitylabels != null && entitylabels.size() != 0) {
						for(String label : entitylabels) {
							set.add(label + SEPARATOR + predicate);
						}
					}
				}   
			} 
			return set;
		} else 
			return null;
	}
	
	public Set<String> computeNeighbors(String entityUri) throws IOException {
		HashSet<String> set = new HashSet<String>(); 

		TermQuery tqfw = new TermQuery(new Term(LuceneGraphStorage.FIELD_SRC, entityUri));
		TermQuery tqbw = new TermQuery(new Term(LuceneGraphStorage.FIELD_DST, entityUri));
		
		Hits hits = dataSearcher.search(tqfw);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String predicate = doc.get(LuceneGraphStorage.FIELD_EDGE);
				String object = doc.get(LuceneGraphStorage.FIELD_DST);
				String type = TypeUtil.checkType(predicate, object);
				if(type.equals(TypeUtil.RELATION))
					set.add(object);
			}
		} 
		
		hits = dataSearcher.search(tqbw);
		if(hits != null && hits.length() != 0) {
			Iterator iter = hits.iterator();
			while(iter.hasNext()) {
				Document doc = ((Hit)iter.next()).getDocument();
				String predicate = doc.get(LuceneGraphStorage.FIELD_EDGE);
				String subject = doc.get(LuceneGraphStorage.FIELD_SRC);
				String type = TypeUtil.checkType(predicate, entityUri);
				if(type.equals(TypeUtil.RELATION))
					set.add(subject);
			}
		} 
		
		return set;
	} 
	
	public Set<String> computeReachableEntities(String entityUri) throws IOException {
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
