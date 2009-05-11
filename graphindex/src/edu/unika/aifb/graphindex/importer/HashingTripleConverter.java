package edu.unika.aifb.graphindex.importer;

import edu.unika.aifb.graphindex.util.Util;

public class HashingTripleConverter extends AbstractTripleConverter {

	public HashingTripleConverter(HashedTripleSink hashedSink) {
		super(hashedSink);
	}

	public void triple(String s, String p, String o, String objectType) {
		m_hashedSink.triple(Util.hash(s), Util.hash(p), Util.hash(o), objectType);
	}
}
