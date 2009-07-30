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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneIndexStorage;

public class DataIndex extends Index {

	public interface NodeListener {
		public void node(String node);
	}
	
	public DataIndex(IndexDirectory idxDirectory, IndexConfiguration idxConfig) throws IOException {
		super(idxDirectory, idxConfig);
		m_is = new LuceneIndexStorage(idxDirectory.getDirectory(IndexDirectory.VP_DIR));
		m_is.initialize(false, true);
	}
	
	public IndexDescription getCompatibleIndex(DataField... fields) {
		for (IndexDescription index : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES))
			if (index.isCompatible(fields))
				return index;
		return null;
	}
	
	public IndexDescription getSuitableIndex(DataField... fields) {
		for (IndexDescription index : m_idxConfig.getIndexes(IndexConfiguration.DI_INDEXES))
			if (index.getIndexFieldMap(fields) != null)
				return index;
		return null;
	}
	
	public IndexStorage getIndexStorage() {
		return m_is;
	}

	public GTable<String> getTriples(String s, String p, String o) throws StorageException {
		List<DataField> indexFields = new ArrayList<DataField>();
		Map<DataField,String> indexValues = new HashMap<DataField,String>();

		if (s != null) {
			indexFields.add(DataField.SUBJECT);
			indexValues.put(DataField.SUBJECT, s);
		}
		
		if (p != null) {
			indexFields.add(DataField.PROPERTY);
			indexValues.put(DataField.PROPERTY, p);
		}
		
		if (o != null) {
			indexFields.add(DataField.OBJECT);
			indexValues.put(DataField.OBJECT, o);
		}
		
		IndexDescription index = getSuitableIndex(indexFields.toArray(new DataField[] {}));
		if (index == null) {
			throw new UnsupportedOperationException("no suitable index found");
		}
		
		return m_is.getTable(index, new DataField[] { DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT }, index.createValueArray(indexValues));
	}

	public GTable<String> getQuads(String s, String p, String o, String c) throws StorageException {
		List<DataField> indexFields = new ArrayList<DataField>();
		Map<DataField,String> indexValues = new HashMap<DataField,String>();

		if (s != null) {
			indexFields.add(DataField.SUBJECT);
			indexValues.put(DataField.SUBJECT, s);
		}
		
		if (p != null) {
			indexFields.add(DataField.PROPERTY);
			indexValues.put(DataField.PROPERTY, p);
		}
		
		if (o != null) {
			indexFields.add(DataField.OBJECT);
			indexValues.put(DataField.OBJECT, o);
		}
		
		if (c != null) {
			indexFields.add(DataField.CONTEXT);
			indexValues.put(DataField.CONTEXT, c);
		}
		
		IndexDescription index = getSuitableIndex(indexFields.toArray(new DataField[] {}));
		if (index == null) {
			throw new UnsupportedOperationException("no suitable index found");
		}
		
		return m_is.getTable(index, new DataField[] { DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT, DataField.CONTEXT }, index.createValueArray(indexValues));
	}

	public Map<String,Set<String>> getImage(String node, boolean preimage) throws StorageException {
		IndexDescription index = getSuitableIndex(preimage ? DataField.OBJECT : DataField.SUBJECT);
		GTable<String> table = m_is.getTable(index, new DataField[] { preimage ? DataField.SUBJECT : DataField.OBJECT } , 
				index.createValueArray(preimage ? DataField.OBJECT : DataField.SUBJECT, node));
		
		Map<String,Set<String>> image = new HashMap<String,Set<String>>();
		for (String[] row : table) {
			Set<String> nodes = image.get(row[0]);
			if (nodes == null) {
				nodes = new HashSet<String>();
				image.put(row[0], nodes);
			}
			nodes.add(row[1]);
 		}
		
		return image;
	}

	public Set<String> getImage(String node, String property, boolean preimage) throws StorageException {
		DataField df = preimage ? DataField.OBJECT : DataField.SUBJECT;
		IndexDescription index = getSuitableIndex(DataField.PROPERTY, df);
		return m_is.getDataSet(index, preimage ? DataField.SUBJECT : DataField.OBJECT, index.createValueArray(DataField.PROPERTY, property, df, node));
	}
	
	public Set<String> getSubjectNodes(String property) throws StorageException {
		IndexDescription index = getSuitableIndex(DataField.PROPERTY);
		return m_is.getDataSet(index, DataField.SUBJECT, index.createValueArray(DataField.PROPERTY, property));
	}

	public Set<String> getObjectNodes(String property) throws StorageException {
		IndexDescription index = getSuitableIndex(DataField.PROPERTY);
		return m_is.getDataSet(index, DataField.OBJECT, index.createValueArray(DataField.PROPERTY, property));
	}
	
	public void iterateNodes(String property, NodeListener nl) throws StorageException {
		for (String node : getSubjectNodes(property))
			nl.node(node);
		for (String node : getObjectNodes(property))
			nl.node(node);
	}
	
	public Iterator<String[]> iterator(String property) throws StorageException {
		return m_is.iterator(getSuitableIndex(DataField.PROPERTY), new DataField[] { DataField.SUBJECT, DataField.PROPERTY, DataField.OBJECT }, property);
	}
}
