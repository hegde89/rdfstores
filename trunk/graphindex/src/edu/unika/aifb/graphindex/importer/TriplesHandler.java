/**
 * 
 */
package edu.unika.aifb.graphindex.importer;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

class TriplesHandler implements RDFHandler {
		private int m_triplesTotal;
		private int m_triplesAdded;
		private TripleSink m_sink;

		public TriplesHandler(TripleSink sink) {
			m_sink = sink;
		}

		public void endRDF() throws RDFHandlerException {
		}

		public void handleComment(String arg0) throws RDFHandlerException {
		}

		public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		}

		public void handleStatement(Statement st) throws RDFHandlerException {
			m_triplesTotal++;
			
			String label = st.getPredicate().toString();
			String source = null; 
			
			if (st.getSubject() instanceof org.openrdf.model.URI) {
				source = ((org.openrdf.model.URI)st.getSubject()).toString();
			}
			else if (st.getSubject() instanceof BNode) {
				BNode bn = (BNode)st.getSubject();
				source = bn.getID();
//				log.debug(source);
				if (!source.startsWith("http"))
					source = "_:" + source;
			}
			else {
				NTriplesImporter.log.warn("subject is not an URI or a blank node, ignoring " + st.getSubject().getClass());
				return;
			}
			
			String target = null;
			if (st.getObject() instanceof org.openrdf.model.URI) {
				target = ((org.openrdf.model.URI)st.getObject()).toString();
			}
			else if (st.getObject() instanceof Literal) {
				Literal l = (Literal)st.getObject();
//				log.debug("datatype: " + l.getDatatype());
//				if (l.getDatatype() != null)
//					target = l.getDatatype().toString();
				target = l.stringValue();
				target = target.replaceAll("\n", "\\\\" + "n");
			}
			else if (st.getObject() instanceof BNode) {
				BNode bn = (BNode)st.getObject();
				target = bn.getID();
				if (!target.startsWith("http"))
					target = "_:" + target;
//				log.debug(target + " " + label + " " + source);
			}
			else {
				NTriplesImporter.log.warn("object is not an URI, a literal or a blank node, ignoring " + st);
				return;
			}
			
			if (source != null && target != null && label != null) {
				m_sink.triple(source, label, target, null);
				m_triplesAdded++;
//				if (m_triplesAdded % 500000 == 0)
//					log.debug("nt importer: " + m_triplesAdded + " triples imported");
			}
			else {
				NTriplesImporter.log.debug(source + " " + label + " " + target);
			}
		}

		public void startRDF() throws RDFHandlerException {
		}
	}