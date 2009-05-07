package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.util.Util;

public class MiscTest {
	class P {
		public void setP(P p) {
			System.out.println("p");
		}
	}
	class F extends P {
		public void setP(F f) {
			System.out.println("f");
		}
	}
	class M extends P {
		public void setP(P m) {
			super.setP(m);
			System.out.println("m");
		}
	}
	
	public void test() {
		M adam = new M();
		F eva = new F();
		
		P p = adam;
		
		p.setP(eva);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(Util.hash("http://www.Department3.University0.edu/AssociateProfessor6/Publication5"));
		System.out.println(Util.hash("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name"));
		System.out.println(Util.hash("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor"));
		MiscTest t = new MiscTest();
		t.test();
		System.exit(-1);
		List<Long> list = new ArrayList<Long>();
		for (int i = 0; i < 10000000; i++)
			list.add((long)i);
		System.gc();
		System.out.println(Util.memory());

		list = null;
		System.gc();
		System.out.println(Util.memory());
		
		Long[] wa = new Long[10000000];
		for (int i = 0; i < wa.length; i++)
			wa[i] = (long)i;
		System.gc();
		System.out.println(Util.memory());
		
		wa = null;
		System.gc();
		System.out.println(Util.memory());
		
		long[] la = new long[10000000];
		for (int i = 0; i < la.length; i++)
			la[i] = (long)i;
		System.gc();
		System.out.println(Util.memory());
		
//		List<Long> list = new LinkedList<Long>();
//		
//		for (int i = 0; i < 10000000; i++) {
//			list.add((long)i);
//		}
//		
//		System.out.println(Util.memory());
//		list = null;
//		System.gc();
//		System.out.println(Util.memory());
//		
//		Set<Long> set = new HashSet<Long>();
//		for (int i = 0; i < 10000000; i++) {
//			set.add((long)i);
//		}
//		System.out.println(set.size());
//		System.out.println(Util.memory());
//		set = null;
//		System.gc();
//		System.out.println(Util.memory());
	}

}
