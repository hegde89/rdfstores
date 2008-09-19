/**
 * 
 */
package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.storage.Triple;

public class TripleSet {
		private Set<Triple> m_triples;
		private static final Logger log = Logger.getLogger(TripleSet.class);
		
		public TripleSet() {
			this(new HashSet<Triple>());
		}
		
		public TripleSet(Set<Triple> triples) {
			m_triples = triples;
		}
		
		public Set<Triple> getTriples() {
			return m_triples;
		}
		
		public void addTriples(Set<Triple> triples) {
			m_triples.addAll(triples);
		}
		
		public Set<String> getSubjects() {
			Set<String> subjects = new HashSet<String>();
			for (Triple t : m_triples)
				subjects.add(t.getSubject());
			return subjects;
		}
		
		public Set<String> getObjects() {
			Set<String> objects = new HashSet<String>();
			for (Triple t : m_triples)
				objects.add(t.getObject());
			return objects;
		}
		
		public void refineWith(Set<String> objects) {
			for (Iterator<Triple> i = iterator(); i.hasNext(); ) {
				if (!objects.contains(i.next().getObject()))
					i.remove();
			}
		}
		
		public void addToMapping(Set<String> mapped, Set<Map<String,String>> mappings, String term, List<String> sources, List<String> targets, Map<String,TripleSet> tripleMappings) {
			List<String> mappedSources = new ArrayList<String>();
			for (String s : sources)
				if (mapped.contains(s))
					mappedSources.add(s);
			
			List<String> mappedTargets = new ArrayList<String>();
			for (String t : targets)
				if (mapped.contains(t))
					mappedTargets.add(t);
			
			if (sources.size() == 0 && mappings.size() == 0) {
				for (String o : getObjects()) {
					Map<String,String> map = new HashMap<String,String>();
					map.put(term, o);
					mappings.add(map);
				}
			}
			else if (mappedSources.size() == 0 && mappings.size() > 0) {
//				log.debug("sources: " + sources + ", targets: " + targets);
				
				if (mappedTargets.size() == 0) {
					log .error("at least one target should be mapped");
				}
				else {
					Set<Map<String,String>> newMappings = new HashSet<Map<String,String>>();
					for (Iterator<Map<String,String>> i = mappings.iterator(); i.hasNext(); ) {
						Map<String,String> mapping = i.next();
//						log.debug("mapping: " + mapping);
						List<Triple> toAdd = new ArrayList<Triple>();
						for (Triple t : m_triples) {
//							log.debug(" t: " + t);
							for (String target : mappedTargets) {
								String targetMapped = mapping.get(target);
//								log.debug("  target: " + target + ", targetMapped: " + targetMapped);
								for (Triple targetTriple : tripleMappings.get(target).getTriples()) {
									if (targetTriple.getObject().equals(targetMapped) && targetTriple.getSubject().equals(t.getObject())) {
//										log.debug("   " + targetTriple + " valid");
										if (!mapping.values().contains(targetTriple.getSubject())) {
											toAdd.add(t);
											break;
										}
									}
								}
							}
						}
//						log.debug("toAdd: " + toAdd);
						i.remove();
						
						if (toAdd.size() > 0) {
							for (Triple t : toAdd) {
								Map<String,String> newMap = new HashMap<String,String>(mapping);
								newMap.put(term, t.getObject());
								newMappings.add(newMap);
							}
						}
					}
					mappings.addAll(newMappings);
				}
			}
			else if (mappedSources.size() > 0) {
				Map<String,List<Triple>> obj2t = new HashMap<String,List<Triple>>();
				for (Triple t : m_triples) {
					List<Triple> ts = obj2t.get(t.getObject());
					if (ts == null) {
						ts = new ArrayList<Triple>();
						obj2t.put(t.getObject(), ts);
					}
					ts.add(t);
				}
				
				Set<Map<String,String>> newMappings = new HashSet<Map<String,String>>();
				for (Iterator<Map<String,String>> j = mappings.iterator(); j.hasNext(); ) {
					Map<String,String> mapping = j.next();
					
//					log.debug("mapping: " + mapping);
					List<Triple> toAdd = new ArrayList<Triple>();
					if (sources.size() > 1) {
						List<String> sourcesMapped = new ArrayList<String>();
						for (String sourceV : mappedSources)
							sourcesMapped.add(mapping.get(sourceV));
//						log.debug("sourcesMapped: " + sourcesMapped);
						
						for (List<Triple> ts : obj2t.values()) {
//							log.debug("ts: " + ts);
							if (ts.size() < sourcesMapped.size())
								continue;
							
							boolean all = true;
							for (String sourceMapped : sourcesMapped) {
								boolean found = false;
								for (Triple t : ts) {
									if (t.getSubject().equals(sourceMapped)) {
										found = true;
										break;
									}
								}
								if (!found) {
									all = false;
									break;
								}
							}
//							log.debug("all: " + all);
							if (all)
								toAdd.add(ts.get(0));
						}
					}
					else {
						String sourceMapped = mapping.get(sources.get(0));
//						log.debug("sourceV: " + sources.get(0) + ", sourceMapped: " + sourceMapped);
						for (Triple t : m_triples) {
							if (t.getSubject().equals(sourceMapped)) {
//								log.debug("t to add: " + t);
								toAdd.add(t);
							}
						}
					}
					
					j.remove();
					
					if (toAdd.size() > 0) {
						for (Triple t : toAdd) {
							Map<String,String> newMap = new HashMap<String,String>(mapping);
							newMap.put(term, t.getObject());
							newMappings.add(newMap);
						}
					}
					
//					mapping.put(term, toAdd.get(0).getObject());
				}
//				log.debug("newMappings: " + newMappings);
				mappings.addAll(newMappings);
//				log.debug("mappings: " + mappings);
			}
		}
		
		public Iterator<Triple> iterator() {
			return m_triples.iterator();
		}

		public void addTriple(Triple triple) {
			m_triples.add(triple);
		}

		public int size() {
			return m_triples.size();
		}
		
		public String toString() {
			return m_triples.toString();
		}
	}