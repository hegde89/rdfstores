package edu.unika.aifb.graphindex.preprocessing;

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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Pseudograph;

import edu.unika.aifb.graphindex.importer.HashedTripleSink;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.util.Util;

public class TriplesPartitioner implements HashedTripleSink {
	
	private Map<Long,Integer> h2p;
	private Map<Integer,Set<Integer>> pmap;
	private int m_pid;
	private int m_triples = 0;
	private String m_componentDirectory;
	private final int MERGE_INTERVAL = 2500000;
	private final int PCOUNT_INTERVAL = 50000000;
	private final int STATUS_INTERVAL = 50000;
	private boolean m_partitioningDisabled = false;
	private static final Logger log = Logger.getLogger(TriplesPartitioner.class);
	
	public TriplesPartitioner(String componentDirectory) throws IOException {
		h2p = new HashMap<Long,Integer>();
		pmap = new HashMap<Integer,Set<Integer>>();
		m_pid = 0;
		m_componentDirectory = componentDirectory;
		
		File[] cf = new File(componentDirectory).listFiles();
		if (cf != null)
			for (File f : new File(componentDirectory).listFiles()) {
				if (f.getName().startsWith("component")) {
					f.delete();
					log.debug("deleted " + f);
				}
			}
	}
	
	public void triple(long s, long p, long o, String objectType) {
		long sh = s;
		long oh = o;
		
		if (!m_partitioningDisabled) {
			// both uris are known
			if (h2p.containsKey(sh) && h2p.containsKey(oh)) {
				int spid = h2p.get(sh);
				int opid = h2p.get(oh);
				if (spid != opid) {
					// uris are in different partitions, mark partitions as being one partition
					Set<Integer> plist = pmap.get(spid);
					if (plist == null) {
						plist = new HashSet<Integer>();
						pmap.put(spid, plist);
					}
					plist.add(opid);
				}
			}
			else if (h2p.containsKey(sh) && !h2p.containsKey(oh)) {
				// add the object to the partition of the subject
				h2p.put(oh, h2p.get(sh));
			}
			else if (!h2p.containsKey(sh) && h2p.containsKey(oh)) {
				// add the subject to the partition of the object
				h2p.put(sh, h2p.get(oh));
			}
			else {
				// both unknown -> create new partition
				h2p.put(sh, m_pid);
				h2p.put(oh, m_pid);
				m_pid++;
			}
		}
		else {
			h2p.put(sh, m_pid);
			h2p.put(oh, m_pid);
		}
		
		if (m_triples % MERGE_INTERVAL == 0 && !m_partitioningDisabled)
			mergePartitions();
		
		if (m_triples % PCOUNT_INTERVAL == 0)
			log.debug("components: " + partitionCount());
		
		if (m_triples % STATUS_INTERVAL == 0)
			log.debug(m_triples + " " + h2p.size() + " " + pmap.size());
		
		m_triples++;
	}
	
	private void mergePartitions() {
		UndirectedGraph<Integer,DefaultEdge> g = new Pseudograph<Integer,DefaultEdge>(DefaultEdge.class);
		
		// convert partition map to a graph
		// - partitions are nodes
		// - an edge between partitions signifies that the partitions need to be merged
		for (int p1 : pmap.keySet())
			for (int p2 : pmap.get(p1)) {
				g.addVertex(p1);
				g.addVertex(p2);
				g.addEdge(p1, p2);
			}
		
		m_pid++;
		Map<Integer,Integer> old2new = new HashMap<Integer,Integer>();
		
		// all partitions in a connected component are combined to one new partition
		ConnectivityInspector<Integer,DefaultEdge> ci = new ConnectivityInspector<Integer,DefaultEdge>(g);
		for (Set<Integer> component : ci.connectedSets()) {
			for (int p : component)
				old2new.put(p, m_pid);
			m_pid++;
		}
		
		for (long h : h2p.keySet()) {
			int pid = h2p.get(h);
			if (old2new.get(pid) != null) 
				h2p.put(h, old2new.get(pid));
		}
		
		pmap.clear();
	}
	
	private int partitionCount() {
		Set<Integer> partitions = new HashSet<Integer>();
		for (int i : h2p.values())
			partitions.add(i);
		return partitions.size();
	}
	
	public void write(){
		mergePartitions();
		
		Map<Integer,Integer> p2c = new HashMap<Integer,Integer>();
		Map<Integer,PrintWriter> p2f = new HashMap<Integer,PrintWriter>();
		
		for (long h : h2p.keySet()) {
			int p = h2p.get(h);
		
			// accumulate partition sizes
			if (!p2c.containsKey(p))
				p2c.put(p, 0);
			p2c.put(p, p2c.get(p) + 1);
			
			// open a PrinterWriter for partition p
			PrintWriter pw;
			if (!p2f.containsKey(p)) {
				try {
					File file = new File(m_componentDirectory + "/component" + p);
					if (!file.exists()) 
						file.getParentFile().mkdirs(); 
					pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
					p2f.put(p, pw);
//					pw = new PrintWriter(new BufferedWriter(new FileWriter(m_componentDirectory + "/component" + p, true)));
//					p2f.put(p, pw);
				}
				catch (IOException e) {
					// exceptions are probably due to too many open files
					// close all open file descriptors and try again
					// TODO configuration option to specify maximum open files
					log.debug(e);
					for (PrintWriter w : p2f.values())
						w.close();
					p2f.clear();
					
					try {
						pw = new PrintWriter(new BufferedWriter(new FileWriter(m_componentDirectory + "/component" + p, true)));
						p2f.put(p, pw);
					} catch (IOException e1) {
						e1.printStackTrace();
						return;
					}
				}
			}
			else
				pw = p2f.get(p);
			
			pw.println(h);
		}
		
		for (PrintWriter pw : p2f.values())
			pw.close();
		
		int min = Integer.MAX_VALUE, max = 0, avg = 0;
		for (int c : p2c.values()) {
			if (c > max)
				max = c;
			if (c < min)
				min = c;
			avg += c;
		}
		avg /= p2c.size();
		
		log.info("triples: " + m_triples + ", components (total/min/max/avg): " + partitionCount() + "/" + min + "/" + max + "/" + avg);
	}

	public void disablePartitioning(boolean b) {
		m_partitioningDisabled = true;
	}
}
