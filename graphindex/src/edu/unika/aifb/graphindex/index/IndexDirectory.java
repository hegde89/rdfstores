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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexDirectory {
	private static final Map<Integer,String> m_directories = new HashMap<Integer,String>();
	private static final Map<Integer,String> m_files = new HashMap<Integer,String>();
	private static int m_dirIdx = 0, m_fileIdx = 0;
	
	private static int addDirectory(String dir) {
		m_dirIdx++;
		m_directories.put(m_dirIdx, dir);
		return m_dirIdx;
	}

	private static int addFile(String file) {
		m_fileIdx++;
		m_files.put(m_fileIdx, file);
		return m_fileIdx;
	}
	
	public static final int CONFIG_FILE = addFile("config.yml");
	public static final int PROPERTIES_FILE = addFile("properties");
	public static final int OBJECT_PROPERTIES_FILE = addFile("object_properties");
	public static final int DATA_PROPERTIES_FILE = addFile("data_properties");
	public static final int BW_EDGESET_FILE = addFile("backward_edgeset");
	public static final int FW_EDGESET_FILE = addFile("forward_edgeset");
	public static final int CARDINALITIES_FILE = addFile("cardinalities");
	
	public static final int SP_IDX_DIR = addDirectory("sp_idx");
	public static final int SP_GRAPH_DIR = addDirectory("sp_graph");
	public static final int VP_DIR = addDirectory("vp");
	public static final int KEYWORD_DIR = addDirectory("keyword");
	public static final int BDB_DIR = addDirectory("bdb");
	public static final int TEMP_DIR = addDirectory("temp");
	public static final int NEIGHBORHOOD_DIR = addDirectory("neighborhood");
	
	public static final int FACET_TREE_DIR = addDirectory("facets"+ "/"+"trees");
	public static final int FACET_OBJECTS_DIR = addDirectory("facets"+ "/"+"objects");
	public static final int FACET_TEMP_DIR = addDirectory("facets"+ "/"+"temp");	
	public static final int FACET_SEARCH_LAYER_CACHE = addDirectory("facets"+ "/"+"fsl");
	
	private String m_directory;
	
	public IndexDirectory(String dir) throws IOException {
		m_directory = dir;
	}
	
	public void create() {
		new File(m_directory).mkdirs();
	}
	
	public File getFile(int file) throws IOException {
		return getFile(file, false);
	}
	
	public File getFile(int file, boolean truncate) throws IOException {
		File f = new File(m_directory + "/" + m_files.get(file));
		
		if (truncate) {
			f.delete();
			f.createNewFile();
		}
		
		return f;
	}
	
	public File getTempFile(String name, boolean truncate) throws IOException {
		File f = new File(getDirectory(TEMP_DIR) + "/" + name);
		
		if (truncate) {
			f.delete();
			f.createNewFile();
		}
		
		return f;
	}
	
	public File getDirectory(int dir) throws IOException {
		return getDirectory(dir, false);
	}
	
	public File getDirectory(int dir, boolean empty) throws IOException {
		String directory = m_directory + "/" + m_directories.get(dir);
		
		if (empty) {
			File f = new File(directory);
			if (!f.exists())
				f.mkdirs();
			else
				emptyDirectory(f);
		}
		
		return new File(directory);
	}
	
	private void emptyDirectory(File dir) {
		for (File f : dir.listFiles()) {
			if (f.isDirectory())
				emptyDirectory(f);
			else
				f.delete();
		}
	}
}
