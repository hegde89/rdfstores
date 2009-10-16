package edu.unika.aifb.graphindex.searcher.keyword.analyzer;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Set;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class CapitalizationSplitterAnalyzer extends StandardAnalyzer {
	private Set stopSet;

	public static final String[] STOP_WORDS = StopAnalyzer.ENGLISH_STOP_WORDS;

	public CapitalizationSplitterAnalyzer() {
		this(STOP_WORDS);
	}

	public CapitalizationSplitterAnalyzer(Set stopWords) {
		stopSet = stopWords;
	}

	public CapitalizationSplitterAnalyzer(String[] stopWords) {
		stopSet = StopFilter.makeStopSet(stopWords);
	}

	public CapitalizationSplitterAnalyzer(File stopwords) throws IOException {
		stopSet = WordlistLoader.getWordSet(stopwords);
	}

	public CapitalizationSplitterAnalyzer(Reader stopwords) throws IOException {
		stopSet = WordlistLoader.getWordSet(stopwords);
	}

	public TokenStream reusableTokenStream(String fieldName, Reader reader)
			throws IOException {
		SavedStreams streams = (SavedStreams) getPreviousTokenStream();
		if (streams == null) {
			streams = new SavedStreams();
			setPreviousTokenStream(streams);
			streams.tokenStream = new StandardTokenizer(reader);
			streams.filteredTokenStream = new StandardFilter(streams.tokenStream);
			streams.filteredTokenStream = new CapitalizationSplitterTokenFilter(streams.filteredTokenStream);
			streams.filteredTokenStream = new LowerCaseFilter(streams.filteredTokenStream);
			streams.filteredTokenStream = new StopFilter(streams.filteredTokenStream, stopSet);
		} else {
			streams.tokenStream.reset(reader);
		}
		streams.tokenStream.setMaxTokenLength(getMaxTokenLength());

		streams.tokenStream.setReplaceInvalidAcronym(getDefaultReplaceInvalidAcronym());

		return streams.filteredTokenStream;
	}

	public TokenStream tokenStream(String fieldName, Reader reader) {
		StandardTokenizer tokenStream = new StandardTokenizer(reader, getDefaultReplaceInvalidAcronym());
		tokenStream.setMaxTokenLength(getMaxTokenLength());
		TokenStream result = new StandardFilter(tokenStream);
		result = new CapitalizationSplitterTokenFilter(tokenStream);
		result = new LowerCaseFilter(result);
		result = new StopFilter(result, stopSet);
		return result;
	}

	private static final class SavedStreams {
		StandardTokenizer tokenStream;
		TokenStream filteredTokenStream;
	}

}
