package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MiscTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(Util.memory());
		List<Long> list = new LinkedList<Long>();
		
		for (int i = 0; i < 10000000; i++) {
			list.add((long)i);
		}
		
		System.out.println(Util.memory());
		list = null;
		System.gc();
		System.out.println(Util.memory());
		
		Set<Long> set = new HashSet<Long>();
		for (int i = 0; i < 10000000; i++) {
			set.add((long)i);
		}
		System.out.println(set.size());
		System.out.println(Util.memory());
		set = null;
		System.gc();
		System.out.println(Util.memory());
	}

}
