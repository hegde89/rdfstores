package edu.unika.aifb.graphindex.searcher.keyword.analyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class HyphenationCompoundWordStandardAnalyzer extends StandardAnalyzer {
	
	private HyphenationTree hyphenator;
	private Set<String> dict;
	
	public HyphenationCompoundWordStandardAnalyzer(String grammer, String dictionary) throws Exception {
		super();
		BufferedReader br = new BufferedReader(new FileReader(dictionary));
		String line;
		dict = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			dict.add(line);
		}
		br.close();

		br = new BufferedReader(new FileReader(grammer));
		hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(br);
		br.close();
	}
	
	public HyphenationCompoundWordStandardAnalyzer(String grammer, String dictionary, String stopWords) throws Exception {
		super(new File(stopWords));
		BufferedReader br = new BufferedReader(new FileReader(dictionary));
		String line;
		dict = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			dict.add(line);
		}
		br.close();

		br = new BufferedReader(new FileReader(grammer));
		hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(br);
	}
	
	public HyphenationCompoundWordStandardAnalyzer(File grammer, File dictionary) throws Exception {
		super();
		BufferedReader br = new BufferedReader(new FileReader(dictionary));
		String line;
		dict = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			dict.add(line);
		}
		br.close();

		br = new BufferedReader(new FileReader(grammer));
		hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(br);
		br.close();
	}
	
	public HyphenationCompoundWordStandardAnalyzer(File grammer, File dictionary, File stopWords) throws Exception {
		super(stopWords);
		BufferedReader br = new BufferedReader(new FileReader(dictionary));
		String line;
		dict = new HashSet<String>();
		while ((line = br.readLine()) != null) {
			dict.add(line);
		}
		br.close();

		br = new BufferedReader(new FileReader(grammer));
		hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(br);
	}
	
	public TokenStream tokenStream(String field, Reader reader) {
		return new HyphenationCompoundWordTokenFilter(
				super.tokenStream(field, reader), hyphenator, dict,
		CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,
		CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,
		CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, false);
	}
	
	public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
		return tokenStream(fieldName, reader);
	}
}