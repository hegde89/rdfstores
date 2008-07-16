package edu.unika.aifb.graphindex.importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

public class NTriplesImporter extends Importer {

	private int m_triplesTotal = 0, m_triplesAdded = 0;
	
	private class TriplesHandler implements RDFHandler {
		public void endRDF() throws RDFHandlerException {
		}

		public void handleComment(String arg0) throws RDFHandlerException {
		}

		public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		}

		public void handleStatement(Statement st) throws RDFHandlerException {
			m_triplesTotal++;
			
			if (!(st.getSubject() instanceof org.openrdf.model.URI)) {
				log.warn("subject is not an URI, ignored " + st.getSubject().getClass());
				return;
			}

			String label = st.getPredicate().toString();
			String source = ((org.openrdf.model.URI)st.getSubject()).toString();
			
			String target = null;
			if (st.getObject() instanceof org.openrdf.model.URI) {
				target = ((org.openrdf.model.URI)st.getObject()).toString();
			}
			else if (st.getObject() instanceof Literal) {
				Literal l = (Literal)st.getObject();
//				log.debug("datatype: " + l.getDatatype());
				if (l.getDatatype() != null)
					target = l.getDatatype().toString();
			}
			else {
				log.warn("ignoring data properties " + st);
				return;
			}
			
			if (source != null && target != null && label != null) {
				m_gb.addTriple(source, label, target);
				m_triplesAdded++;
			}
		}

		public void startRDF() throws RDFHandlerException {
		}
	}
	
	public NTriplesImporter() {
		super();
		log = Logger.getLogger(NTriplesImporter.class);
	}
	
	@Override
	public void doImport() {
		RDFHandler handler = new TriplesHandler();
		
		for (String file : m_files) {
			NTriplesParser parser = new NTriplesParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			
			try {
				parser.parse(new BufferedReader(new FileReader(file), 10000000), "");
			} catch (RDFParseException e) {
				e.printStackTrace();
			} catch (RDFHandlerException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			log.info("triples: " + m_triplesAdded + "/" + m_triplesTotal);
		}
	}

}
