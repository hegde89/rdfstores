package edu.unika.aifb.graphindex.searcher.entity;

/**
 * Copyright (C) 2009 G�nter Ladwig (gla at aifb.uni-karlsruhe.de)
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.searcher.Searcher;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraph;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraphNode;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class EntityLoader extends Searcher {
	private int m_cutoff = -1;
	private IndexStorage m_is;
	
	private static final Logger log = Logger.getLogger(EntityLoader.class);
	
	public EntityLoader(IndexReader reader) throws StorageException, IOException {
		super(reader);
		m_is = reader.getDataIndex().getIndexStorage();
	}
	
	public void setCutoff(int cutoff) {
		m_cutoff = cutoff;
	}
	
	public TransformedGraph loadEntities(TransformedGraph tg, NeighborhoodStorage ns) throws IOException, StorageException {
		for (TransformedGraphNode node : tg.getNodes()) {
			List<GTable<String>> tables = new ArrayList<GTable<String>>();
			for (String property : node.getAttributeQueries().keySet()) {
				Collection<String> objects = node.getAttributeQueries().get(property);
				for (String object : objects) {
					GTable<String> table = m_is.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, property, object);
					table.setColumnName(0, node.getNodeName());
					table.setColumnName(1, object);
					tables.add(table);
				}
			}
			
			for (String type : node.getTypeQueries()) {
				GTable<String> table = m_is.getIndexTable(IndexDescription.POS, DataField.SUBJECT, DataField.OBJECT, RDF.TYPE.toString(), type);
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
				int i = 0;
				for (String[] row : table) {
					entities.add(new KeywordElement(new Entity(row[col]), KeywordElement.ENTITY, ns));
					i++;
					if (m_cutoff > 0 && i >= m_cutoff)
						break;
				}
				log.debug("entities after cutoff: " + entities.size());
				node.setEntities(entities);
			}
		}
		
		return tg;
	}
}
