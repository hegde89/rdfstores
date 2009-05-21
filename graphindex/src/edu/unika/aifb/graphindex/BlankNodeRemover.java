package edu.unika.aifb.graphindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BlankNodeRemover {
	public static void removeBlankNode(String fn, String noBNFile) throws IOException
	{
		Set<String> conEdgeSet = new HashSet<String>();
		String[] containerEdge = new String[]{
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#List",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#nil"};
		for (String e : containerEdge) {
			conEdgeSet.add(e);
		}
		String blankNode = "_:";
		String fileName = new File(fn).getName();
		String blankNodeFile = fileName + ".blanknodes";//fn.substring(0, fn.lastIndexOf('.'))+".blanknodes";
		String noBlankNodeFile = fileName + ".noblanknode.nt";//fn.substring(0, fn.lastIndexOf('.'))+".noblanknode.nt";
		noBlankNodeFile = noBNFile;
		Map<String, String> blankNodeMap = new HashMap<String, String>();
		Set<String> bnMeansCollection = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(fn));
		PrintWriter pw1 = new PrintWriter(new FileWriter(blankNodeFile));
		PrintWriter pw2 = new PrintWriter(new FileWriter(noBlankNodeFile));
		String line;
		int lines = 0;
		while((line = br.readLine())!=null)
		{
			line = line.trim();
			if (line.equals(""))
				continue;
			String[] parts = line.replaceAll("<", "").replaceAll(">", "").split(" ");
			if (parts.length < 3) {
				for (String s : parts)
					System.out.print(s + " ");
				System.out.println();
			}
			if(parts[0].startsWith(blankNode) && !parts[2].startsWith(blankNode))
			{
				if(bnMeansCollection.contains(parts[0]))
					pw1.println(line);
				else if(parts[1].equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && conEdgeSet.contains(parts[2]))
					bnMeansCollection.add(parts[0]);
				
			}
			else if(!parts[0].startsWith(blankNode) && parts[2].startsWith(blankNode))
				blankNodeMap.put(parts[2], "<"+parts[0]+"> <"+parts[1]+">");
			else if(!parts[0].startsWith(blankNode) && !parts[2].startsWith(blankNode))
				pw2.println(line);
			
			lines++;
			if (lines % 1000000 == 0)
				System.out.println("bn removed: " + lines);
		}
		br.close();
		pw1.close();
		br = new BufferedReader(new FileReader(blankNodeFile));
		while((line = br.readLine())!=null)
		{
			String[] parts = line.split(" ");
			if(blankNodeMap.containsKey(parts[0]))
				pw2.println(blankNodeMap.get(parts[0])+" "+parts[2]+" .");
		}
		pw2.close();
		br.close();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length == 2)
			removeBlankNode(args[0], args[1]);
	}
}
