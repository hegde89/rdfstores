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
package edu.unika.aifb.facetedSearch.demo;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionFactory;
import edu.unika.aifb.facetedSearch.store.impl.GenericRdfStore.GenericQueryEvaluator;

/**
 * @author andi
 * 
 */
public class Demo {

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

		SearchSessionFactory searchSessionFactory = SearchSessionFactory
				.getInstance(props);

		try {

			SearchSession session = searchSessionFactory.getSession(searchSessionFactory
					.acquire());

			@SuppressWarnings("unused")
			GenericQueryEvaluator eval = session.getStore().getEvaluator();

			// TODO

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
