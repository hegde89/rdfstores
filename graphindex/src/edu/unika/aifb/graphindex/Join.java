package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Join {
	public static class Tuple {
		Object[] m_data;
		
		public Tuple(Object[] data) {
			m_data = data;
		}
		
		public Object[] getData() {
			return m_data;
		}
		
		public int length() {
			return m_data.length;
		}
		
		@Override
		public int hashCode() {
			int h = 0;
			for (Object o : m_data)
				h += o.hashCode();
			return h;
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Tuple))
				return false;
			
			Tuple t = (Tuple)o;
			
			if (t.length() != this.length())
				return false;
			
			for (int i = 0; i < m_data.length; i++) {
				if (!m_data[i].equals(t.getData()[i]))
					return false;
			}
			
			return true;
		}
		
		@Override
		public String toString() {
			String s = "(";
			String comma = "";
			for (Object o : m_data) {
				s += comma + o;
				comma = ",";
			}
			return s + ")";
		}
	}
	
	public static ResultSet hashJoin(String[] joinVars, ResultSet left, ResultSet right) {

		if (joinVars == null || joinVars.length == 0 || left == null || right == null) return null;
		// always use the smaller set to generate the hash
		if (left.size() < right.size()) {
			ResultSet t = left;
			left = right;
			right = t;
		}
		
		Map<Tuple,List<Result>> t2r = new HashMap<Tuple,List<Result>>();
		
		for (Iterator<Result> i = right.iterator(); i.hasNext(); ) {
			Result r = i.next();
			Tuple t = new Tuple(r.get(joinVars));
			
			List<Result> rl = t2r.get(t);
			if (rl == null) {
				rl = new ArrayList<Result>();
				t2r.put(t, rl);
			}
			rl.add(r);
		}
		
		Set<String> joinVarsSet = new HashSet<String>(Arrays.asList(joinVars));
		List<String> joinedVarsList = new ArrayList<String>();
		List<String> leftVarsList = new ArrayList<String>();
		List<String> rightVarsList = new ArrayList<String>();
		
		for (String v : left.getVars())
			if (!joinVarsSet.contains(v))
				leftVarsList.add(v);
		
		for (String v : right.getVars())
			if (!joinVarsSet.contains(v))
				rightVarsList.add(v);

		joinedVarsList.addAll(leftVarsList);
		joinedVarsList.addAll(rightVarsList);
		joinedVarsList.addAll(Arrays.asList(joinVars));
		
		String[] joinedVars = joinedVarsList.toArray(new String[] {});
		String[] leftVars = leftVarsList.toArray(new String[] {});
		String[] rightVars = rightVarsList.toArray(new String[] {});
		
		ResultSet result = new ResultSet(joinedVars);
		
		for (Iterator<Result> i = left.iterator(); i.hasNext(); ) {
			Result lr = i.next();
			Tuple lt = new Tuple(lr.get(joinVars));
			
			List<Result> rrs = t2r.get(lt);
			if (rrs != null) {
				for (Result rr : rrs) {
					Object[] newRow = new Object [joinedVars.length];
					System.arraycopy(lr.get(leftVars), 0, newRow, 0, leftVars.length);
					System.arraycopy(rr.get(rightVars), 0, newRow, leftVars.length, rightVars.length);
					System.arraycopy(lt.getData(), 0, newRow, leftVars.length + rightVars.length, joinVars.length);
					result.addResult(new Result(result, newRow));
				}
			}
		}
		
		return result;
	}
	
	public static void printSet(ResultSet r) {
		for (String v : r.getVars())
			System.out.print(v + "\t");
		System.out.println();
		for (Iterator<Result> i = r.iterator(); i.hasNext(); ) {
			Result row = i.next();
			for (int j = 0; j < r.getVars().length; j++)
				System.out.print(row.get(j) + "\t");
			System.out.println();
		}
	}
	
	public void test() {
		ResultSet rs1 = new ResultSet(new String[] {"a", "b", "c" });
		ResultSet rs2 = new ResultSet(new String[] {"d", "b", "c" });
		
		rs1.addResult(new String[] {"1", "2", "3"});
		rs1.addResult(new String[] {"3", "5", "5"});
		rs1.addResult(new String[] {"1", "3", "2"});
		rs1.addResult(new String[] {"6", "3", "4"});
		rs1.addResult(new String[] {"0", "3", "4"});
	
		rs2.addResult(new String[] {"2", "3", "2"});
		rs2.addResult(new String[] {"2", "3", "4"});
		rs2.addResult(new String[] {"5", "5", "4"});
		rs2.addResult(new String[] {"1", "3", "3"});
		rs2.addResult(new String[] {"4", "3", "7"});
		
		printSet(rs1);
		printSet(rs2);
		
		ResultSet res = Join.hashJoin(new String[] {"c", "b"}, rs1, rs2);
	
		printSet(res);
	}

	public static void main(String[] args) {
		Join j = new Join();
		j.test();
	}
}
