package edu.unika.aifb.graphindex.searcher.keyword.analyzer;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;

public class CapitalizationSplitterTokenFilter extends TokenFilter {

	protected final LinkedList tokens;

	protected CapitalizationSplitterTokenFilter(TokenStream input) {
		super(input);
		this.tokens = new LinkedList();
	}

	public Token next(final Token reusableToken) throws IOException {
		assert reusableToken != null;
		if (tokens.size() > 0) {
			return (Token) tokens.removeFirst();
		}

		Token nextToken = input.next(reusableToken);
		if (nextToken == null) {
			return null;
		}

		decompose(nextToken);

		if (tokens.size() > 0) {
			return (Token) tokens.removeFirst();
		} else {
			return null;
		}
	}

	protected final Token createToken(final int offset, final int length, final Token prototype) {
		int newStart = prototype.startOffset() + offset;
		Token t = prototype.clone(prototype.termBuffer(), offset, length, newStart, newStart + length);
		t.setPositionIncrement(0);
		return t;
	}

	protected void decompose(final Token token) {
		tokens.add((Token) token.clone());

		char[] termBuffer = token.termBuffer();
		int i = 0, j = Math.min(1, token.termLength() - 1);
		
		while(j < token.termLength() - 1) {
			if(Character.isUpperCase(termBuffer[j])) {
				tokens.add(createToken(i, j - i, token));
				i = j++;
			}
			else 
				j++;
		}
		tokens.add(createToken(i, token.termLength() - i, token));
	}
}
