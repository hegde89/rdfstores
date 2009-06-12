package edu.unika.aifb.keywordsearch.search;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.keywordsearch.KeywordSegement;

/**
 * 
 */
public class KeywordPartitioner {
	private static final Logger log = Logger.getLogger(KeywordPartitioner.class);

	public static Iterator<Set<String>> segementIterator(final Set<String> values) {
		return new Iterator<Set<String>>() {
			private int i = 1;
			private long n = 1 << Math.max(0, values.size());

			public boolean hasNext() {
				return i < n;
			}
			
			public Set<String> next() {
				Set<String> set = new HashSet<String>();
				int j = 0;
				for(String e : values) {
					if (((1 << j++) & i) != 0) {
						set.add(e);
					}
				}
				++i;
				return set;
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	public static <T> boolean addAll(Collection<KeywordSegement> collection, Iterator<Set<String>> iterator) {
		if(collection != null) {
			boolean wasModified = false;
			while(iterator.hasNext()) {
				wasModified |= collection.add(new KeywordSegement(iterator.next()));
			}
			return wasModified;
		}
		return false;
	}

	public static Set<KeywordSegement> getSegements(Set<String> keywords) {
		Set<KeywordSegement> segements = new HashSet<KeywordSegement>();
		addAll(segements, segementIterator(keywords));
		return segements;
	}

	public static SortedSet<KeywordSegement> getOrderedSegements(Set<String> keywords) {
		SortedSet<KeywordSegement> orderedSegements = new TreeSet<KeywordSegement>();
		Iterator<Set<String>> segementIterator = segementIterator(keywords);
		while(segementIterator.hasNext()) {
			KeywordSegement segement = new KeywordSegement(segementIterator.next());
			orderedSegements.add(segement);
		}
		return orderedSegements;
	}

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();

			String tokens[] = line.split(" ");
			HashSet<String> set = new HashSet<String>();
			for (int i = 0; i < tokens.length; i++) {
				set.add(tokens[i]);
			}

			SortedSet<KeywordSegement> segements = getOrderedSegements(set);
			log.debug("added " + set);
			log.debug("powerset = " + segements);

		}

	}

}
