package edu.unika.aifb.graphindex.storage;

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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.GTable;

public interface IndexStorage {
	public void initialize(boolean clean, boolean readonly);
	public void close() throws StorageException;
	public void reopen() throws StorageException;
	
	public void addData(IndexDescription index, String[] indexKeys, Collection<String> values) throws StorageException;
	public void addData(IndexDescription index, String[] indexKeys, List<String> values, boolean sort) throws StorageException;
	public void addData(IndexDescription index, String[] indexKeys, String value) throws StorageException;
	
	public boolean hasValues(IndexDescription index, String... indexFields) throws StorageException;
	
	public List<String> getDataList(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException;
	public Set<String> getDataSet(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException;
	public String getDataItem(IndexDescription index, DataField field, String... indexFieldValues) throws StorageException;
	
	public GTable<String> getTable(IndexDescription index, DataField[] columns, String... indexFieldValues) throws StorageException;
	public GTable<String> getIndexTable(IndexDescription index, DataField col1, DataField col2, String... indexFieldValues) throws StorageException;
	
	public void mergeIndex(IndexDescription index) throws StorageException;
	public void optimize() throws StorageException;

	public Iterator<String[]> iterator(IndexDescription index, DataField[] columns, String... indexFieldValues) throws StorageException;
}
