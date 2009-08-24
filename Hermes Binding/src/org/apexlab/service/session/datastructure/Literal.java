package org.apexlab.service.session.datastructure;

/**
 * This class represents an RDF litteral
 * @author tpenin
 */
public class Literal extends Facet {
   
	/**
	 * Default constructor
	 */
	public Literal() {
		super();
	}
	
	/**
	 * Constructor
	 * @param label The label of the litteral
	 * @param uri The URI of the litteral
	 * @param source The source of the litteral
	 */
	public Literal(String label, String uri, Source source) {
		super(label, uri, source);
	}

}
