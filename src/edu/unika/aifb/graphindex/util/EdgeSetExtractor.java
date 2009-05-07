package edu.unika.aifb.graphindex.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;


public class EdgeSetExtractor {

	private String datasource;
	private String outFile;
	private Set<String> set;
	
	public EdgeSetExtractor(String datasource, String outFile){
		this.datasource = datasource;
		this.outFile = outFile;
		this.set = new TreeSet<String>();
	}
	
	public void extract() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(datasource));
		PrintWriter	pw = new PrintWriter(new FileWriter(outFile));
		
		System.out.println("Extracting ...");
		String line;
		while((line = br.readLine())!=null)
		{
			String[] parts = line.replaceAll("<", "").replaceAll(">", "").split(" ");
			set.add(parts[1]);
			if(set.size() > 10000) {
				for(String edge : set) {
					pw.println(edge);
				}
			}
			
		}
		if(set.size() != 0) {
			for(String edge : set) {
				pw.println(edge);
			}
		}
		
		br.close();
		pw.close();
		
		System.out.println("Sorting ...");
		LineSortFile lsf = new LineSortFile(outFile);
		lsf.setDeleteWhenStringRepeated(true);
		lsf.sortFile();
	} 
	
	
	public static void main(String[] args) {
		EdgeSetExtractor ex = new EdgeSetExtractor(args[0],args[1]);
		try {
			ex.extract();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
