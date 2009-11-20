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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.rdfxml.RDFXMLParser;

public class RDFImporter extends Importer {
	private static Logger log = Logger.getLogger(RDFImporter.class);

	public RDFImporter() {
		super();
	}

	@Override
	public void doImport() {
		TriplesHandler handler = new TriplesHandler(m_sink,
				ignoreDataTypesEnabled(), super.getDefaultDataType());

		for (String file : m_files) {
			handler.setDefaultContext(file);

			RDFXMLParser parser = new RDFXMLParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			parser.setVerifyData(false);

			try {
				parser.parse(
						new BufferedReader(new FileReader(file), 10000000), "");
			} catch (RDFParseException e) {
				e.printStackTrace();
			} catch (RDFHandlerException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
