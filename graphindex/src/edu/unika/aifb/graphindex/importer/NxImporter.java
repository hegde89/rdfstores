package edu.unika.aifb.graphindex.importer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class NxImporter extends Importer {
	private static Logger log = Logger.getLogger(NxImporter.class);

	@Override
	public void doImport() {
		try {
			for (String file : m_files) {
				log.debug("file: " + file);
				NxParser nxp = new NxParser(new FileInputStream(file));

				while (nxp.hasNext()) {
					Node[] nodes = nxp.next();

					String subject = null, property = null, object = null, context = null;

					if (nodes[0] instanceof Resource) {
						subject = ((Resource) nodes[0]).toString();
					} else if (nodes[0] instanceof BNode) {
						subject = ((BNode) nodes[0]).toString();
					} else
						log.error("subject is neither a resource nor a bnode");

					if (nodes[1] instanceof Resource) {
						property = ((Resource) nodes[1]).toString();
					} else
						log.error("property is not a resource");

					if (nodes[2] instanceof Resource) {
						object = ((Resource) nodes[2]).toString();
					} else if (nodes[2] instanceof BNode) {
						object = ((BNode) nodes[2]).toString();
					} else if (nodes[2] instanceof Literal) {

						String datatype = ((Literal) nodes[2]).getDatatype() == null ? super
								.getDefaultDataType()
								: ((Literal) nodes[2]).getDatatype().toString();

						object = super.ignoreDataTypesEnabled() ? ((Literal) nodes[2])
								.getData()
								: ((Literal) nodes[2]).getData()
										+ Character.toString((char) 94)
										+ datatype;
					} else
						log.error("object is not a resource, bnode or literal");

					if (object == null || object.equals("")) {
						log.warn("object empty, ignoring statement");
						continue;
					}
						
					
					if (nodes.length > 3) {
						if (nodes[3] instanceof Resource) {
							context = ((Resource) nodes[3]).toString();
						} else
							log.error("context is not a resource");
					} else
						context = file;

					m_sink.triple(subject, property, object, context);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
