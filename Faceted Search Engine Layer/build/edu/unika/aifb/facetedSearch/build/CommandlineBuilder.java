/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.build;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.facetedSearch.FacetEnvironment;
import edu.unika.aifb.facetedSearch.FacetedSearchLayerConfig;
import edu.unika.aifb.facetedSearch.index.FacetIndexCreator;
import edu.unika.aifb.graphindex.index.IndexDirectory;

/**
 * @author andi
 * 
 */
public class CommandlineBuilder {

	public static void main(String[] args) {

		OptionParser op = new OptionParser();
		op.accepts("c", "Path to config file.").withRequiredArg().ofType(
				String.class);

		OptionSet os = op.parse(args);

		if (!os.has("c")) {
			try {
				op.printHelpOn(System.out);
			} catch (IOException e) {
				return;
			}
			return;
		}

		FileReader fileReader = null;

		try {
			fileReader = new FileReader((String) os.valueOf("c"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		Properties props = new Properties();

		try {
			props.load(fileReader);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {

			IndexDirectory idxDir = new IndexDirectory(props
					.getProperty(FacetEnvironment.Property.GRAPH_INDEX_DIR));

			FacetedSearchLayerConfig.setFacetIdxDirStrg(props
					.getProperty(FacetEnvironment.Property.FACET_INDEX_DIR));

			FacetIndexCreator fic = new FacetIndexCreator(idxDir, props
					.getProperty(FacetEnvironment.Property.EXPRESSIVITY),
					FacetedSearchLayerConfig.getFacetTreeIdxDir());
			fic.create();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
