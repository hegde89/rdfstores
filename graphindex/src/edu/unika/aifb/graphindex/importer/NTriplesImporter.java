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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

public class NTriplesImporter extends Importer {

	private boolean m_removeBN = true;
	
	public NTriplesImporter() {
		this(true);
	}
	
	public NTriplesImporter(boolean rmBN) {
		super();
		m_removeBN = rmBN;
		log = Logger.getLogger(NTriplesImporter.class);
	}
	
	public String removeBlankNode(String fn) throws IOException
	{
		Set<String> conEdgeSet = new HashSet<String>();
		String[] containerEdge = new String[]{
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#List",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"};
		for (String e : containerEdge) {
			conEdgeSet.add(e);
		}
		String blankNode = "_:";
		String fileName = new File(fn).getName();
		String blankNodeFile = fileName + ".blanknodes";//fn.substring(0, fn.lastIndexOf('.'))+".blanknodes";
		String noBlankNodeFile = fileName + ".noblanknode.nt";//fn.substring(0, fn.lastIndexOf('.'))+".noblanknode.nt";
		Map<String, String> blankNodeMap = new HashMap<String, String>();
		Set<String> bnMeansCollection = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(fn));
		PrintWriter pw1 = new PrintWriter(new FileWriter(blankNodeFile));
		PrintWriter pw2 = new PrintWriter(new FileWriter(noBlankNodeFile));
		String line;
		while((line = br.readLine())!=null)
		{
			line = line.trim();
			if (line.equals(""))
				continue;
			String[] parts = line.replaceAll("<", "").replaceAll(">", "").split(" ");
			if(parts[0].startsWith(blankNode) && !parts[2].startsWith(blankNode))
			{
				if(bnMeansCollection.contains(parts[0]))
					pw1.println(line);
				else if(parts[1].equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && conEdgeSet.contains(parts[2]))
					bnMeansCollection.add(parts[0]);
				
			}
			else if(!parts[0].startsWith(blankNode) && parts[2].startsWith(blankNode))
				blankNodeMap.put(parts[2], "<"+parts[0]+"> <"+parts[1]+">");
			else if(!parts[0].startsWith(blankNode) && !parts[2].startsWith(blankNode))
				pw2.println(line);
		}
		br.close();
		pw1.close();
		br = new BufferedReader(new FileReader(blankNodeFile));
		while((line = br.readLine())!=null)
		{
			String[] parts = line.split(" ");
			if(blankNodeMap.containsKey(parts[0]))
				pw2.println(blankNodeMap.get(parts[0])+" "+parts[2]+" .");
		}
		pw2.close();
		br.close();
		
		return noBlankNodeFile;
	}
	
	@Override
	public void doImport() {
		RDFHandler handler = new TriplesHandler(m_sink);
		
		for (String file : m_files) {
			
			try {
				if (m_removeBN) {
					log.debug("removing blank nodes...");
					file = removeBlankNode(file);
					log.debug("done");
				}
				
				NTriplesParser parser = new NTriplesParser();
				parser.setDatatypeHandling(DatatypeHandling.VERIFY);
				parser.setStopAtFirstError(false);
				parser.setRDFHandler(handler);
				parser.setPreserveBNodeIDs(true);
				
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
			
//			log.info("triples: " + m_triplesAdded + "/" + m_triplesTotal);
		}
	}

}
