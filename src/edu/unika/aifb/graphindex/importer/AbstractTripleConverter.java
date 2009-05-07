package edu.unika.aifb.graphindex.importer;

public abstract class AbstractTripleConverter implements TripleSink {

	protected HashedTripleSink m_hashedSink;
	
	public AbstractTripleConverter(HashedTripleSink hashedSink) {
		m_hashedSink = hashedSink;
	}
}
