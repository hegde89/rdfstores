package edu.unika.aifb.graphindex.storage.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

public class LuceneWarmer {
	public static Set<String> getWarmupTerms(String directory, int count) throws CorruptIndexException, IOException {
		IndexReader ir = IndexReader.open(directory);
		
		int numTerms = 0;
		TermEnum te = ir.terms();
		while (te.next())
			numTerms++;
		te.close();
		
		Random r = new Random();
		Set<Integer> termIndexes = new HashSet<Integer>();
		for (int i = 0; i < count; i++)
			termIndexes.add(r.nextInt(numTerms));

		Set<String> terms = new HashSet<String>();
		int i = 0;
		te = ir.terms();
		while (te.next()) {
			Term t = te.term();
			if (termIndexes.contains(i))
				terms.add(t.text());
			i++;
		}
		te.close();
		
		return terms;
	}

	public static Set<String> getWarmupTerms(String directory, String field, int count) throws CorruptIndexException, IOException {
		IndexReader ir = IndexReader.open(directory);
		
		int numTerms = 0;
		TermEnum te = ir.terms(new Term(field, ""));
		do {
			if (!te.term().field().equals(field))
				break;
			numTerms++;
		}
		while (te.next());
		te.close();
		
		numTerms = Math.max(1, numTerms);
		
		Random r = new Random();
		Set<Integer> termIndexes = new HashSet<Integer>();
		for (int i = 0; i < count; i++)
			termIndexes.add(r.nextInt(numTerms));

		Set<String> terms = new HashSet<String>();
		int i = 0;
		te = ir.terms(new Term(field, ""));
		do {
			Term t = te.term();
			
			if (!t.field().equals(field))
				break;
			
			if (termIndexes.contains(i))
				terms.add(t.text());
			i++;
		}
		while (te.next());
		te.close();
		
		return terms;
	}
}
