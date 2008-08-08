package edu.unika.aifb.graphindex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphFactory;

public class Util {
	
	public static String digest(String uri) {
		try{
			MessageDigest algorithm = MessageDigest.getInstance("MD5");
			algorithm.update(uri.getBytes());
			byte messageDigest[] = algorithm.digest();
		            
			StringBuffer hexString = new StringBuffer();
			for (int i=0;i<messageDigest.length;i++) {
				hexString.append(Integer.toHexString(0xFF & messageDigest[i]));
			}
			return "_" + hexString.toString(); 
		} catch(NoSuchAlgorithmException nsae){
		}
		
		return uri.replaceAll("\\/|:|\\.|#|\\?|&|\\+|-|~", "_");
	}

	public static String truncateUri(String uri) {
		if (uri == null)
			return null;
		int idx = uri.lastIndexOf("#");
		if (idx < 0)
			idx = uri.lastIndexOf("/");
		if (idx < 0)
			return uri;
		return uri.substring(idx + 1);
	}
	
	public static void printDOT(Writer out, Graph graph) {
		try {
			out.write(graph.toDot());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void printDOT(String file, Graph graph) {
		try {
			printDOT(new FileWriter(file), graph);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printDOT(Graph graph) {
		printDOT(new PrintWriter(System.out), graph);
	}
	
	public static void writeObject(String file, Object object) throws FileNotFoundException, IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
		out.writeObject(object);
		out.close();
	}

	public static boolean pathContains(List<String> path, String source, String label, String target) {
		for (int i = 0; i < path.size() - 2; i += 2) {
			if (path.get(i).equals(source)) {
				if (path.get(i + 1).equals(label) && path.get(i + 2).equals(target))
					return true;
			}
		}
		return false;
	}

	public static Object readObject(String string) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(string));
		Object o = ois.readObject();
		ois.close();
		return o;
	}
	
	public static long freeMemory() {
		Runtime r = Runtime.getRuntime();
		return (r.freeMemory() + (r.maxMemory() - r.totalMemory())) / 1000;
	}

	public static Object[] permute(int k, Object[] os) {
		for (int j = 2; j <= os.length; j++) {
			k = k / (j - 1);
			Object tmp = os[j - 1];
			os[j - 1] = os[(k % j)];
			os[(k % j)] = tmp;
		}
		return os;
	}

	public static int factorial(int n)
	{
	    if( n <= 1 )
	        return 1;
	    else
	        return n * factorial( n - 1 );
	}
	
	public static Graph loadGT(String file) throws IOException {
		Graph g = GraphFactory.graph();
		
		BufferedReader in = new BufferedReader(new FileReader(file));
		String input;
		while ((input = in.readLine()) != null) {
			String[] t = input.split("\t");
			g.addEdge(t[0], t[1], t[2]);
		}
		return g;
	}
}
