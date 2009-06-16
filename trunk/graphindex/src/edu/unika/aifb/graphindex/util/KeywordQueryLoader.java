package edu.unika.aifb.graphindex.util;

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
