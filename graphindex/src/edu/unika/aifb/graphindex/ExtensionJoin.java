package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.Triple;

public class ExtensionJoin {
	public static class TriplePair {
		public Triple left;
		public Triple right;
		
		public TriplePair(Triple left, Triple right) {
			this.left = left;
			this.right = right;
		}
		
		public String toString() {
			return left + "," + right;
		}
	}
	
	/**
	 * left.subject = right.object
	 * 
	 * @param left
	 * @param leftProperty
	 * @param right
	 * @return
	 * @throws StorageException
	 */
	public static List<TriplePair> join(Extension left, String leftProperty, Extension right) throws StorageException {
		Set<Triple> leftTriples = left.getTriples(leftProperty);
		Set<Triple> rightTriples = right.getTriples();
		
		// TODO use smaller triple set to create hash table (can't just swap, because we use
		// different columns on each side)
		
		Map<String,List<Triple>> leftSubject2Triples = new HashMap<String,List<Triple>>();
		for (Triple lt : leftTriples) {
			List<Triple> triples = leftSubject2Triples.get(lt.getSubject());
			if (triples == null) {
				triples = new ArrayList<Triple>();
				leftSubject2Triples.put(lt.getSubject(), triples);
			}
			triples.add(lt);
		}
		
		List<TriplePair> result = new ArrayList<TriplePair>();
		
		for (Triple rt : rightTriples) {
			List<Triple> lts = leftSubject2Triples.get(rt.getObject());
			if (lts != null && lts.size() > 0) {
				for (Triple lt : lts) {
					result.add(new TriplePair(lt, rt));
				}
			}
		}
		
		return result;
	}
}
