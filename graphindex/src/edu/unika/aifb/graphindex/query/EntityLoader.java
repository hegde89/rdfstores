package edu.unika.aifb.graphindex.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.LuceneStorage.Index;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.impl.Entity;

public class EntityLoader {
	
	private LuceneStorage m_ls;
	
	private static final Logger log = Logger.getLogger(EntityLoader.class);
	
	public EntityLoader(String directory) throws StorageException {
		m_ls = new LuceneStorage(directory);
		m_ls.initialize(false, true);
	}
	
	public TransformedGraph loadEntities(TransformedGraph tg, NeighborhoodStorage ns) throws IOException, StorageException {
		for (TransformedGraphNode node : tg.getNodes()) {
			List<GTable<String>> tables = new ArrayList<GTable<String>>();
			for (String property : node.getAttributeQueries().keySet()) {
				Collection<String> objects = node.getAttributeQueries().get(property);
				for (String object : objects) {
					GTable<String> table = m_ls.getIndexTable(Index.PO, property, object);
					table.setColumnName(0, node.getNodeName());
					table.setColumnName(1, object);
					tables.add(table);
				}
			}
			
			for (String type : node.getTypeQueries()) {
				GTable<String> table = m_ls.getIndexTable(Index.PO, RDF.TYPE.toString(), type);
				table.setColumnName(0, node.getNodeName());
				table.setColumnName(1, type);
				tables.add(table);
			}
			
			if (tables.size() > 1) {
				PriorityQueue<GTable<String>> pq = new PriorityQueue<GTable<String>>(tables.size(), new Comparator<GTable<String>>() {
					public int compare(GTable<String> o1, GTable<String> o2) {
						return ((Integer)o1.rowCount()).compareTo(o2.rowCount());
					}
				});
				
				pq.addAll(tables);
				
				while (pq.size() > 1) {
					GTable<String> t1 = pq.poll();
					GTable<String> t2 = pq.poll();
					
					GTable<String> res = Tables.mergeJoin(t1, t2, node.getNodeName());
					
					pq.add(res);
				}

				tables.clear();
				tables.add(pq.peek());
			}
			
			if (tables.size() > 0) {
				GTable<String> table = tables.get(0);
				log.debug("table for " + node.getNodeName() + ": " + table);
				
				List<KeywordElement> entities = new ArrayList<KeywordElement>(table.rowCount());
				int col = table.getColumn(node.getNodeName());
				for (String[] row : table) {
					entities.add(new KeywordElement(new Entity(row[col]), KeywordElement.ENTITY, ns));
				}
				node.setEntities(entities);
			}
		}
		
		return tg;
	}
}
