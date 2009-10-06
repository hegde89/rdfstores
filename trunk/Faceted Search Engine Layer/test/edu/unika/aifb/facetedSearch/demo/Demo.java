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
import edu.unika.aifb.facetedSearch.search.evaluator.GenericQueryEvaluator;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;
import edu.unika.aifb.facetedSearch.search.session.SearchSessionFactory;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.query.StructuredQuery;

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

			SearchSession session = searchSessionFactory
					.getSession(searchSessionFactory.acquire());

			GenericQueryEvaluator eval = session.getStore().getEvaluator();

			// StructuredQuery sq = new StructuredQuery("q1");
			// sq
			// .addEdge("?x",
			// "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
			// "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
			// sq
			// .addEdge(
			// "?x",
			// "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom",
			// "?y");
			//
			// eval.evaluate(sq);
			//
			// StructuredQuery sq2 = new StructuredQuery("q2");
			// sq2
			// .addEdge("?x",
			// "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
			// "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication");
			//
			// eval.evaluate(sq2);

			StructuredQuery sq3 = new StructuredQuery("q3");
			sq3
					.addEdge(
							"?x",
							"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse",
							"?y");

			sq3
					.addEdge(
							"?x",
							"http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf",
							"http://www.Department8.University0.edu");

			sq3.setAsSelect("?x");

			eval.evaluate(sq3);

			KeywordQuery kq = new KeywordQuery("q3",
					"Publication0 publicationAuthor GraduateStudent1@Department10.University0.edu");
			eval.evaluate(kq);

			// TODO

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
