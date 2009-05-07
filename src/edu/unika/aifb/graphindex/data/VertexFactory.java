package edu.unika.aifb.graphindex.data;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


public class VertexFactory {
	private static Class m_vertexClass;
	private static Class m_collectionClass;
	
	public static void setVertexClass(Class clazz) {
		m_vertexClass = clazz;
	}
	
	public static void setCollectionClass(Class clazz) {
		m_collectionClass = clazz;
	}
	
	public static IVertex vertex(long id) {
		try {
			Constructor ct = m_vertexClass.getConstructor(Long.TYPE);
			return (IVertex)ct.newInstance(id);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
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

	public static VertexCollection collection() {
		try {
			return (VertexCollection)m_collectionClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		return null;
	}
}
