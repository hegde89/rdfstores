package edu.unika.aifb.graphindex.graph;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import edu.unika.aifb.graphindex.query.QueryGraph;

public class GraphFactory {
	private static int m_id = 0;
	
	private static String nextName() {
		return "graph" + ++m_id;
	}
	
	public static Graph graph() {
		return new Graph(nextName(), m_id);
	}
	
	public static Graph graphByClass(String cls, boolean tryStub) {
		try {
			Class clazz = Class.forName(cls);
			Constructor ct = clazz.getConstructor(String.class, Boolean.TYPE);
			return (Graph)ct.newInstance(nextName(), tryStub);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Graph graphByClass(String cls, String name, boolean tryStub) {
		try {
			Class clazz = Class.forName(cls);
			Constructor ct = clazz.getConstructor(String.class, Integer.class, Boolean.class);
			return (Graph)ct.newInstance(nextName(), m_id, tryStub);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static QueryGraph queryGraph() {
		return new QueryGraph(nextName(), m_id);
	}

	public static Graph graph(String name) {
		return new Graph(name, m_id++);
	}
	
	public static QueryGraph queryGraph(String name) {
		return new QueryGraph(name, m_id++);
	}
}
