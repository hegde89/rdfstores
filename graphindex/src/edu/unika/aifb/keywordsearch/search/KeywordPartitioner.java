package edu.unika.aifb.keywordsearch.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

	public static Iterator<Set<String>> segementIterator(final List<String> values) {
		return new Iterator<Set<String>>() {
			private int i = 1;
			private int size = values.size();
			private long n = 1 << Math.max(0, size);

			public boolean hasNext() {
				return i < n;
			}
			
			public Set<String> next() {
				Set<String> set = new HashSet<String>();
				int j = 0;
				for(int cursor = 0; cursor < size; cursor++) {
					if (((1 << j++) & i) != 0) {
						set.add(values.get(cursor));
					}
				}
				++i;
				
				while(!isSegementCandidate(i) && i < n) 
					++i;
				return set;
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public static Iterator<Set<KeywordSegement>> partitionIterator(final Set<KeywordSegement> values, final Set<String> keywords) {
		return new Iterator<Set<KeywordSegement>>() {
			private int i = 1;
			private int size = values.size();
			private long n = 1 << Math.max(0, size);
			private Set<KeywordSegement> next;
			private Set<Integer> combinations = new HashSet<Integer>() ; 

			public boolean hasNext() {
				if(i < n) {
					boolean found = false;
					while(found == false && i < n ) {
						Set<KeywordSegement> set = new HashSet<KeywordSegement>();
						Set<String> contained = new HashSet<String>();
						int j = 0;
						out: for(KeywordSegement segement : values) {
							if (((1 << j++) & i) != 0) {
								for(String keyword : segement.getKeywords()) {
									if(contained.contains(keyword))
										break out;
								}
								set.add(segement);
								contained.addAll(segement.getKeywords());
							}
						}
						if(contained.containsAll(keywords)){
							combinations.add(i);
							next = set;
							found = true;
						}
						
						++i;
						while(!isPartitionCandidate(combinations, i) && i < n) 
							++i;
					}	
					if(found == true)
						return true;
					else 
						return false;
				}
				else 
					return false;
			}
			
			public Set<KeywordSegement> next() {
				return next;
			}
			
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
	
	public static Iterator<Set<KeywordSegement>> getPartitionIterator(Set<KeywordSegement> values, Set<String> keywords) {
		return partitionIterator(values, keywords);
	}
	
	public static boolean isPartitionCandidate(Set<Integer> combinations, int i) {
		for(int c : combinations) {
			int m = c&i;
			if(m == c)
				return false;
		}
		return true;
	} 
	
	public static boolean isSegementCandidate(int i) {
		int leftZeros = Integer.numberOfLeadingZeros(i);
		int rightZeros = Integer.numberOfTrailingZeros(i);
		int ones = Integer.bitCount(i);
		if(32 == leftZeros + rightZeros + ones)
			return true;
		return false;
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

	public static Set<KeywordSegement> getSegements(List<String> keywords) {
		Set<KeywordSegement> segements = new HashSet<KeywordSegement>();
		addAll(segements, segementIterator(keywords));
		return segements;
	}

	public static SortedSet<KeywordSegement> getOrderedSegements(List<String> keywords) {
		SortedSet<KeywordSegement> orderedSegements = new TreeSet<KeywordSegement>();
		Iterator<Set<String>> segementIterator = segementIterator(keywords);
		while(segementIterator.hasNext()) {
			KeywordSegement segement = new KeywordSegement(segementIterator.next());
			orderedSegements.add(segement);
		}
		return orderedSegements;
	}
	
	public static SortedSet<KeywordSegement> getOrderedSegements(Collection<List<String>> keywords) {
		SortedSet<KeywordSegement> orderedSegements = new TreeSet<KeywordSegement>();
		for(List<String> list : keywords) {
			Iterator<Set<String>> segementIterator = segementIterator(list);
			while(segementIterator.hasNext()) {
				KeywordSegement segement = new KeywordSegement(segementIterator.next());
				orderedSegements.add(segement);
			}
		}
		return orderedSegements;
	}

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();
			
			Collection<List<String>> colOfLists = new ArrayList<List<String>>();
			
			String tokenslists[] = line.split("\t");
			for(String tokenslist : tokenslists) {
				String tokens[] = tokenslist.split(" ");
				List<String> list = new ArrayList<String>();
				for (int i = 0; i < tokens.length; i++) {
					list.add(tokens[i]);
				}
				colOfLists.add(list);
			}

			SortedSet<KeywordSegement> segements = getOrderedSegements(colOfLists);
			log.debug("added " + colOfLists);
			log.debug("segements size: " + segements.size());
			log.debug("segements = " + segements);

		}

	}

}
