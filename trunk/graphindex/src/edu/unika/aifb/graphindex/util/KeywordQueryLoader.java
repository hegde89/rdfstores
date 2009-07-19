package edu.unika.aifb.graphindex.util;

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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.query.KeywordQuery;

public class KeywordQueryLoader {

	public List<KeywordQuery> loadQueryFile(String queryFile) throws IOException {
		List<KeywordQuery> queries = new ArrayList<KeywordQuery>();
		
		BufferedReader in = new BufferedReader(new FileReader(queryFile));
		String input;
		String name = null;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			
			if (input.startsWith("#"))
				continue;

			if (input.startsWith("query")) {
				String[] t = input.split(" ");
				name = t[t.length - 1];
			}
			else if (!input.equals("")) {
				queries.add(new KeywordQuery(name, input));
			}
		}
		
		return queries;
	}

}
