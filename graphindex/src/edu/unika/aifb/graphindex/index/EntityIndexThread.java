package edu.unika.aifb.graphindex.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ThreadListener;
import edu.unika.aifb.graphindex.util.TypeUtil;

public class EntityIndexThread extends Thread {

	private final static Logger log = Logger.getLogger(EntityIndexThread.class);
	ThreadListener listener;
	private KeywordIndexBuilder kwidxbuilder;
	private IndexWriter indexWriter;
	private IndexWriter valueWriter;
	private File efile;
	private IndexReader reader;

	public EntityIndexThread(KeywordIndexBuilder kwidxbuilder,
			IndexWriter indexWriter, File efile, IndexReader reader,
			IndexWriter valueWriter) {
		super();
		this.listener = kwidxbuilder;

		this.kwidxbuilder = kwidxbuilder;
		this.indexWriter = indexWriter;
		this.valueWriter = valueWriter;
		this.efile = efile;
		this.reader = reader;
	}

	public void run() {

		log.debug("Starting entity index thread for file"+efile);

		try {
			indexEntity(indexWriter, efile, reader);
		} catch (StorageException e) {
			e.printStackTrace();
		}

		listener.threadFinished(this);
	}

	private void indexEntity(IndexWriter indexWriter, File efile,
			IndexReader reader) throws StorageException {
		try {
			BufferedReader br = new BufferedReader(new FileReader(efile));
			String line;
			int entities = 0;
			double time = System.currentTimeMillis();
			while ((line = br.readLine()) != null) {
				String uri = line.trim();

				if (reader != null) {
					TermDocs td = reader.termDocs(new Term(Constant.URI_FIELD,
							uri));
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

				// indexWriter.commit();

				entities++;
				if (entities % 100000 == 0) {
					indexWriter.commit();
					valueWriter.commit();
					// ns.commit();
					log.debug("entities indexed: " + entities + " avg: "
							+ ((System.currentTimeMillis() - time) / 100000.0)
							+ " milliseconds per entity.");
					time = System.currentTimeMillis();
				}
			}
			indexWriter.commit();
			valueWriter.commit();
			br.close();

			log.debug(entities + " entities indexed");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected List<Field> getFieldsForEntity(String uri) throws IOException,
			StorageException {
		List<Field> fields = new ArrayList<Field>();

		// indexing type of the element
		fields.add(new Field(Constant.TYPE_FIELD, TypeUtil.ENTITY,
				Field.Store.YES, Field.Index.NO));

		// indexing uri
		fields.add(new Field(Constant.URI_FIELD, uri, Field.Store.YES,
				Field.Index.NOT_ANALYZED_NO_NORMS));

		// indexing extension id
		// if (idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
		// String blockName = blockSearcher.getBlockName(uri);
		// // String blockName = structureIndex.getExtension(uri);
		// if (blockName == null) {
		// log.warn("no ext for " + uri);
		// return null;
		// }
		// fields.add(new Field(Constant.EXTENSION_FIELD, blockName,
		// Field.Store.YES, Field.Index.NO));
		// }

		// indexing concept of the entity element
		// Set<String> concepts = computeConcepts(uri);
		// if(concepts != null && concepts.size() != 0) {
		// for(String concept : concepts) {
		// Field field = new Field(Constant.CONCEPT_FIELD, concept,
		// Field.Store.YES, Field.Index.NO);
		// fields.add(field);
		// }
		// }

		// indexing entity descriptions in value index
		computeEntityDescriptions(uri);

		List<Field> propertyFields = computeProperties(uri);
		fields.addAll(propertyFields);

		// indexing reachable entities
		// Set<String> reachableEntities = computeReachableEntities(uri);
		// BloomFilter bf = new BloomFilter(reachableEntities.size(),
		// NUMBER_HASHFUNCTION);
		// BloomFilter bf = new BloomFilter(reachableEntities.size(),
		// FALSE_POSITIVE);
		// for (String entity : reachableEntities) {
		// if (entity.startsWith("http://www."))
		// entity = entity.substring(11);
		// else if (entity.startsWith("http://"))
		// entity = entity.substring(7);
		// bf.add(entity);
		// }

		// ns.addNeighborhoodBloomFilter(uri, bf);

		return fields;
	}

	private List<Field> computeProperties(String uri) throws StorageException {
		Set<String> inProperties = new HashSet<String>(), outProperties = new HashSet<String>();

		Table<String> table = kwidxbuilder.getDataIndex().getTriples(uri, null,
				null);
		if (table != null && table.rowCount() > 0) {
			for (String[] row : table) {
				if (kwidxbuilder.getObjectProperties().contains(row[1]))
					outProperties.add(row[1]);
			}
		}

		table = kwidxbuilder.getDataIndex().getTriples(null, null, uri);
		if (table != null && table.rowCount() > 0) {
			for (String[] row : table) {
				if (kwidxbuilder.getObjectProperties().contains(row[1]))
					inProperties.add(row[1]);
			}
		}

		// most entities have type anyway
		outProperties.remove(RDF.TYPE.toString());

		List<Field> fields = new ArrayList<Field>();

		StringBuilder sb = new StringBuilder();
		for (String property : inProperties)
			sb.append(property).append('\n');
		fields.add(new Field(Constant.IN_PROPERTIES_FIELD, sb.toString(),
				Field.Store.COMPRESS, Field.Index.NO));

		sb = new StringBuilder();
		for (String property : outProperties)
			sb.append(property).append('\n');
		fields.add(new Field(Constant.OUT_PROPERTIES_FIELD, sb.toString(),
				Field.Store.COMPRESS, Field.Index.NO));

		return fields;
	}

	private void computeEntityDescriptions(String entityUri)
			throws IOException, StorageException {

		String ext = "no structure index created";

		// check if structure index exists
		// if (idxConfig.getBoolean(IndexConfiguration.HAS_SP)) {
		// ext = blockSearcher.getBlockName(entityUri);
		// }

		// Document doc = new Document();
		// doc.add(new Field(Constant.URI_FIELD, entityUri, Field.Store.YES,
		// Field.Index.NO));
		// doc.add(new Field(Constant.EXTENSION_FIELD, ext, Field.Store.YES,
		// Field.Index.NO));
		// doc.add(new Field(Constant.ATTRIBUTE_FIELD,
		// Constant.ATTRIBUTE_LOCALNAME, Field.Store.YES,
		// Field.Index.NOT_ANALYZED_NO_NORMS));
		// Field f = new Field(Constant.CONTENT_FIELD,
		// TypeUtil.getLocalName(entityUri).trim(), Field.Store.NO,
		// Field.Index.ANALYZED);
		// doc.add(f);
		// doc.setBoost(ENTITY_DISCRIMINATIVE_BOOST);
		// valueWriter.addDocument(doc);

		Table<String> table = kwidxbuilder.getDataIndex().getTriples(entityUri,
				null, null);

		// TODO Duplicate aus table rausfiltern!!

		if (table.rowCount() == 0)
			return;
		for (String[] row : table) {
			String predicate = row[1];
			String object = row[2];

			if (!kwidxbuilder.getProperties().contains(predicate)
					|| kwidxbuilder.getRelations().contains(predicate)) { // TODO
				// maybe
				// index
				// relations
				continue;
			}

			Document doc = new Document();
			doc.add(new Field(Constant.URI_FIELD, entityUri, Field.Store.YES,
					Field.Index.NO));
			doc.add(new Field(Constant.ATTRIBUTE_FIELD, predicate,
					Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

			doc.add(new Field(Constant.EXTENSION_FIELD, ext, Field.Store.YES,
					Field.Index.NO));

			float boost = kwidxbuilder.ENTITY_DESCRIPTIVE_BOOST;
			if (predicate.equals(RDFS.LABEL.toString()))
				boost = kwidxbuilder.ENTITY_DISCRIMINATIVE_BOOST;
			if (TypeUtil.getLocalName(predicate).contains("name"))
				boost = kwidxbuilder.ENTITY_DISCRIMINATIVE_BOOST;

			// boost *= propertyWeights.get(predicate);
			doc.setBoost(boost);

			if (kwidxbuilder.getAttributes().contains(predicate)) {
				Field f = new Field(Constant.CONTENT_FIELD, object,
						Field.Store.NO, Field.Index.ANALYZED);
				// f.setBoost(boost);
				doc.add(f);
			} else if (kwidxbuilder.getRelations().contains(predicate)) {
				String localname = TypeUtil.getLocalName(object).trim();

				Field f = new Field(Constant.CONTENT_FIELD, localname,
						Field.Store.NO, Field.Index.ANALYZED);
				// f.setBoost(boost);
				doc.add(f);
			}

			valueWriter.addDocument(doc);
		}

	}

}
