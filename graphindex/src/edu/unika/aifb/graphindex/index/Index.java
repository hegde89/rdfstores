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

import edu.unika.aifb.graphindex.storage.IndexStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public abstract class Index {
//	protected IndexStorage m_is;
	protected IndexDirectory m_idxDirectory;
	protected IndexConfiguration m_idxConfig;
	protected IndexReader m_idxReader;
	protected boolean m_warmup;
	
	public Index(IndexDirectory idxDirectory, IndexConfiguration idxConfig, boolean warmup) {
		m_idxReader = null;
		m_idxDirectory = idxDirectory;
		m_idxConfig = idxConfig;
		m_warmup = warmup;
	}

	public Index(IndexDirectory idxDirectory, IndexConfiguration idxConfig) {
		this(idxDirectory, idxConfig, true);
	}
	
	public Index(IndexReader reader, boolean warmup) {
		m_idxReader = reader;
		m_idxDirectory = reader.getIndexDirectory();
		m_idxConfig = reader.getIndexConfiguration();
		m_warmup = warmup;
	}

	public Index(IndexReader reader) {
		this(reader, true);
	}

	public abstract void close() throws StorageException;
}
