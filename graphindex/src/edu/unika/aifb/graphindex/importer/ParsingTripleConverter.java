package edu.unika.aifb.graphindex.importer;

public class ParsingTripleConverter extends AbstractTripleConverter {

	public ParsingTripleConverter(HashedTripleSink hashedSink) {
		super(hashedSink);
	}

	public void triple(String s, String p, String o, String objectType) {
		m_hashedSink.triple(Long.parseLong(s), Long.parseLong(p), Long.parseLong(o), objectType);
	}
}
