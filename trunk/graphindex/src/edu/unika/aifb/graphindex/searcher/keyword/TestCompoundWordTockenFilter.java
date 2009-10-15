package edu.unika.aifb.graphindex.searcher.keyword;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.apache.lucene.analysis.compound.DictionaryCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;

public class TestCompoundWordTockenFilter {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("./res/en_US.dic"));
		Set<String> dict = new HashSet<String>();
		String line;
		while((line = br.readLine()) != null) {
			dict.add(line);
		}
		br.close();
		
		br = new BufferedReader(new FileReader("./res/en_hyph_US.xml"));
		HyphenationTree hyphenator = HyphenationCompoundWordTokenFilter.getHyphenationTree(br);
		br.close();
		
		HyphenationCompoundWordTokenFilter htf = new HyphenationCompoundWordTokenFilter(
				new WhitespaceTokenizer(new StringReader(
//					"dictionarycompoundwordtokenfilter hyphenationcompoundwordtokenfilter")),
					"birthplace populationtotal areacode areatotal leadertitle populationasof postalcode")),
				hyphenator, dict, 
				CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE,		// 5
				CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE,	// 2
				CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, 	// 15
				false);	
		
		Token ht; 
		int i = 0;
		System.out.println("------------------ Hyphenation Compound Word Token ------------------");
		while ((ht = htf.next()) != null) {
			System.out.println(++i + ": " +  ht);
		}
		
		DictionaryCompoundWordTokenFilter dtf = new DictionaryCompoundWordTokenFilter(
				new WhitespaceTokenizer(new StringReader(
//					"dictionarycompoundwordtokenfilter hyphenationcompoundwordtokenfilter")),
					"birthplace populationtotal areacode areatotal leadertitle populationasof postalcode")),
					dict, 
					CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE, 		// 5 
					CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE, 	// 2
					CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE, 	// 15
					false);
		Token dt; 
		int j = 0;
		System.out.println("------------------ Dictionary Compound Word Token ------------------");
		while ((dt = dtf.next()) != null) {
			System.out.println(++j + ": " + dt);
		}
	}

}
