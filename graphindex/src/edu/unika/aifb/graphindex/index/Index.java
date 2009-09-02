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

import com.sleepycat.je.DatabaseException;

import edu.unika.aifb.graphindex.storage.StorageException;

public abstract class Index {
	// protected IndexStorage m_is;
	protected IndexDirectory m_idxDirectory;
	protected IndexConfiguration m_idxConfig;

	public Index(IndexDirectory idxDirectory, IndexConfiguration idxConfig) {
		this.m_idxDirectory = idxDirectory;
		this.m_idxConfig = idxConfig;
	}

	public abstract void close() throws StorageException, DatabaseException;
}
