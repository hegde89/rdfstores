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

import edu.unika.aifb.graphindex.storage.keyword.BloomFilter;

public interface NeighborhoodStorage {
	public void initialize(boolean clean, boolean readonly) throws StorageException;
	public void close() throws StorageException;
	public void optimize() throws StorageException;
	public void commit() throws StorageException;
	
	public void addNeighborhoodBloomFilter(String uri, BloomFilter filter) throws StorageException;
	
	public BloomFilter getNeighborhoodBloomFilter(String uri) throws StorageException;
}
