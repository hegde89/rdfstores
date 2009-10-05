package edu.unika.aifb.graphindex.importer;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class TriplesHandler implements RDFHandler {
	private static Logger log = Logger.getLogger(TriplesHandler.class);

	private boolean m_ignoreDataTypes;
	private int m_triplesTotal;
	private int m_triplesAdded;
	private String m_defaultContext;
	private String m_defaultDataType;

	private TripleSink m_sink;

	public TriplesHandler(TripleSink sink, boolean ignoreDataTypes,
			String defaultDataType) {
		m_sink = sink;
		m_ignoreDataTypes = ignoreDataTypes;
		m_defaultDataType = defaultDataType;
	}

	public void endRDF() throws RDFHandlerException {
	}

	public void handleComment(String arg0) throws RDFHandlerException {
	}

	public void handleNamespace(String arg0, String arg1)
			throws RDFHandlerException {
	}

	public void handleStatement(Statement st) throws RDFHandlerException {
		m_triplesTotal++;

		String label = st.getPredicate().toString();
		String source = null;

		if (st.getSubject() instanceof org.openrdf.model.URI) {
			source = ((org.openrdf.model.URI) st.getSubject()).toString();
		} else if (st.getSubject() instanceof BNode) {
			BNode bn = (BNode) st.getSubject();
			source = bn.getID();
			// log.debug(source);
			if (!source.startsWith("http")) {
				source = "_:" + source;
			}
		} else {
			log.warn("subject is not an URI or a blank node, ignoring "
					+ st.getSubject().getClass());
			return;
		}

		String target = null;
		if (st.getObject() instanceof org.openrdf.model.URI) {
			target = ((org.openrdf.model.URI) st.getObject()).toString();
		} else if (st.getObject() instanceof Literal) {
			Literal lit = (Literal) st.getObject();
			// log.debug("datatype: " + l.getDatatype());
			// if (l.getDatatype() != null)
			// target = l.getDatatype().toString();

			String dataType = lit.getDatatype() == null ? m_defaultDataType
					: lit.getDatatype().toString();
			
			target = m_ignoreDataTypes ? lit.stringValue()
					: lit.stringValue() + Character.toString((char) 94)
							+ dataType;

			target = target.replaceAll("\n", "\\\\" + "n");
		} else if (st.getObject() instanceof BNode) {
			BNode bn = (BNode) st.getObject();
			target = bn.getID();
			if (!target.startsWith("http")) {
				target = "_:" + target;
				// log.debug(target + " " + label + " " + source);
			}
		} else {
			log
					.warn("object is not an URI, a literal or a blank node, ignoring "
							+ st);
			return;
		}

		if (source.equals("") || target.equals("")) {
			log.warn("subject or object empty, ignoring " + st);
			return;
		}

		String context = m_defaultContext;
		if (st.getContext() != null) {
			context = st.getContext().toString();
		}

		if ((source != null) && (target != null) && (label != null)) {
			m_sink.triple(source, label, target, context);
			m_triplesAdded++;
			// if (m_triplesAdded % 500000 == 0)
			// log.debug("nt importer: " + m_triplesAdded +
			// " triples imported");
		} else {
			log.debug(source + " " + label + " " + target);
		}
	}

	public void setDefaultContext(String context) {
		m_defaultContext = context;
	}

	public void startRDF() throws RDFHandlerException {
	}
}