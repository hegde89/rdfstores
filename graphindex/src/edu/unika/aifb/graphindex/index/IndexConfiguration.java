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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ho.yaml.Yaml;

import edu.unika.aifb.graphindex.storage.IndexDescription;

public class IndexConfiguration {
	public static class Option {
		public String name;
		public Object value;
		
		public Option(String name) {
			this.name = name;
		}
	 
		public Option(String name, Object value) {
			this.name = name;
			this.value = value;
		}
	}
	
	private static final List<Option> m_options = new ArrayList<Option>();
	
	private static Option addOption(String name) {
		Option o = new Option(name);
		m_options.add(o);
		return o;
	}
	
	private static Option addOption(String name, Object value) {
		Option o = new Option(name, value);
		m_options.add(o);
		return o;
	}

	public static final Option IDX_VERSION = addOption("idx_version");
	
	public static final Option HAS_DI = addOption("has_di", true);
	public static final Option HAS_SP = addOption("has_si", true);
	public static final Option HAS_KW = addOption("has_kw", true);
	public static final Option HAS_FI = addOption("has_fi", false);

	public static final Option SP_PATH_LENGTH = addOption("sp_path_length", 1);
	public static final Option SP_DATA_EXTENSIONS = addOption("sp_data_extensions", false);
	public static final Option SP_INDEXES = addOption("sp_indexes");
	public static final Option SP_BACKWARD_ONLY = addOption("sp_backward_only", false);
//	public static final Option SP_INDEX_DATA = addOption("sp_index_data", true);
	
	public static final Option DI_INDEXES = addOption("di_indexes");
	
	public static final Option DP_SP_BASED = addOption("dp_sp_based", true);
	
	public static final Option KW_NSIZE = addOption("kw_nsize", 0);
	
	public IndexConfiguration() throws IOException {
	}
	
	@SuppressWarnings("unchecked")
	public void load(IndexDirectory indexDir) throws IOException {
		Map map = (Map)Yaml.load(indexDir.getFile(IndexDirectory.CONFIG_FILE));
		
		for (Option o : m_options) {
			o.value = map.get(o.name);
			
			if (o.value != null && (o == SP_INDEXES || o == DI_INDEXES)) {
				List<IndexDescription> indexes = new ArrayList<IndexDescription>();
				for (Map indexMap : (List<Map>)o.value) {
					indexes.add(new IndexDescription(indexMap));
				}
				o.value = indexes;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void store(IndexDirectory indexDir) throws FileNotFoundException, IOException {
		Map map = new HashMap();
		
		for (Option o : m_options) {
			if (o == SP_INDEXES || o == DI_INDEXES) {
				List<Map> maps = new ArrayList<Map>();
				for (IndexDescription index : (List<IndexDescription>)o.value) {
					maps.add(index.asMap());
				}
				o.value = maps;
			}
			
			map.put(o.name, o.value);
		}
		
		Yaml.dump(map, indexDir.getFile(IndexDirectory.CONFIG_FILE, true));
	}
	
	public void set(Option o, Object value) {
		o.value = value;
	}
	
	public int getInteger(Option o) {
		return (Integer)o.value;
	}

	public float getFloat(Option o) {
		return (Float)o.value;
	}
	
	public boolean getBoolean(Option o) {
		return (Boolean)o.value;
	}
	
	public String getString(Option o) {
		return (String)o.value;
	}
	
	@SuppressWarnings("unchecked")
	public List getList(Option o) {
		return (List)o.value;
	}
	
	@SuppressWarnings("unchecked")
	public void addIndex(Option o, IndexDescription index) {
		List<IndexDescription> indexes = getList(o);
		if (indexes == null) {
			indexes = new ArrayList<IndexDescription>();
			set(o, indexes);
		}
		
		if (!indexes.contains(index))
			indexes.add(index);
	}
	
	@SuppressWarnings("unchecked")
	public List<IndexDescription> getIndexes(Option o) {
		return getList(o);
	}
}
