package edu.unika.aifb.graphindex.searcher.keyword;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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

import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegement;

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
	
	public static Iterator<Set<KeywordSegement>> partitionIterator(final Collection<KeywordSegement> values, final Collection<String> keywords) {
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

	public static <T> boolean addAllSegements(Collection<KeywordSegement> collection, Iterator<Set<String>> iterator) {
		if(collection != null) {
			boolean wasModified = false;
			while(iterator.hasNext()) {
				wasModified |= collection.add(new KeywordSegement(iterator.next()));
			}
			return wasModified;
		}
		return false;
	}
	
	public static <T> boolean addAllPartitions(Collection<Set<KeywordSegement>> collection, Iterator<Set<KeywordSegement>> iterator, Collection<KeywordSegement> allSegements) {
		if(collection != null) {
			boolean wasModified = false;
			while(iterator.hasNext()) {
				Set<KeywordSegement> segements = iterator.next();
				wasModified |= collection.add(segements);
				allSegements.addAll(segements);
			}
			return wasModified;
		}
		return false;
	}

	public static Set<KeywordSegement> getSegements(List<String> keywords) {
		Set<KeywordSegement> segements = new HashSet<KeywordSegement>();
		addAllSegements(segements, segementIterator(keywords));
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
	
	public static SortedSet<KeywordSegement> getOrderedPartitions(List<String> allKeywords, Collection<Set<KeywordSegement>> partitions) {
		SortedSet<KeywordSegement> orderedSegements = new TreeSet<KeywordSegement>();
		Iterator<Set<String>> segementIterator = segementIterator(allKeywords);
		SortedSet<KeywordSegement> segements = new TreeSet<KeywordSegement>();
		while(segementIterator.hasNext()) {
			KeywordSegement segement = new KeywordSegement(segementIterator.next());
			segements.add(segement);
		}
		addAllPartitions(partitions, partitionIterator(segements, allKeywords), orderedSegements);
		return orderedSegements;
	}
	
	public static SortedSet<KeywordSegement> getOrderedPartitions(Collection<List<String>> allKeywords, Collection<Set<KeywordSegement>> partitions) {
		SortedSet<KeywordSegement> orderedSegements = new TreeSet<KeywordSegement>();
		List<List<Set<KeywordSegement>>> iresults = new ArrayList<List<Set<KeywordSegement>>>();
		for(List<String> list : allKeywords) {
			List<Set<KeywordSegement>> part = new ArrayList<Set<KeywordSegement>>();
			Iterator<Set<String>> segementIterator = segementIterator(list);
			SortedSet<KeywordSegement> segements = new TreeSet<KeywordSegement>();
			while(segementIterator.hasNext()) {
				KeywordSegement segement = new KeywordSegement(segementIterator.next());
				segements.add(segement);
			}
			addAllPartitions(part, partitionIterator(segements, list), orderedSegements);
			iresults.add(part);
		}
		computeCombinations(iresults, partitions);
		return orderedSegements;
	}
	
	public static void computeCombinations(List<List<Set<KeywordSegement>>> iresults, Collection<Set<KeywordSegement>> partitions) {
		int size = iresults.size();
		int[] guards = new int[size];
		for(int i = 0; i < iresults.size(); i++) {
			guards[i] = iresults.get(i).size()-1;
		}
		int[] cursors = new int[size];
		for(int j=0; j<cursors.length; j++) {
			cursors[j] = 0;
		}
		guards[size-1]++;
		do {
			Set<KeywordSegement> combination = new TreeSet<KeywordSegement>();
			for(int m = 0; m < size; m++){
				combination.addAll(iresults.get(m).get(cursors[m]));
			}
			if(partitions.contains(combination))
				System.out.println("==================" + combination + "==========================");
			partitions.add(combination);
			cursors[0]++;
			for(int j = 0; j < size; j++){
				if(cursors[j] > guards[j]){
					cursors[j] = 0;
					cursors[j+1]++; 
				}
			}
		}
		while(cursors[size-1] < guards[size-1]);
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
