package edu.unika.aifb.graphindex.extensions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import edu.unika.aifb.graphindex.Util;

public class FileExtensionStorage implements ExtensionStorageEngine {

	private String m_prefix;
	private String m_extDir;

	public FileExtensionStorage(String dir) {
		m_extDir = dir;
	}
	
	private String getFilename(String uri) {
		return m_extDir + Util.digest(uri) + ".ext";
	}
	
	public Extension readExtension(String uri) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(getFilename(uri)), 10000000);
			
			Extension ext = new Extension(uri);
			String input;
			String currentEdge = "";
			while ((input = in.readLine()) != null) {
				if (input.startsWith("-")) {
					currentEdge = input.substring(1);
					continue;
				}
				
				String[] t = input.split("\t");
				for (int i = 1; i < t.length; i++)
					ext.add(t[0], currentEdge, t[i]);
			}
			return ext;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void storeExtension(Extension ext) {
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(getFilename(ext.getUri()))));
			for (String edgeUri : ext.getEdgeUris()) {
				out.println("-" + edgeUri);
				for (ExtEntry e : ext.getEntries(edgeUri)) {
					out.print(e.getUri());
					for (String parent : e.getParents())
						out.print("\t" + parent);
					out.println();
				}
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	public void removeExtension(String uri) {
		(new File(getFilename(uri))).delete();
	}
	
	public boolean extensionExists(String uri) {
		return (new File(getFilename(uri))).exists();
	}

	public void removeAllExtensions() {
		for (File f : (new File(m_extDir)).listFiles()) {
			f.delete();
		}
	}
	
	public int numberOfExtensions() {
		return 0;
	}

	public void setPrefix(String prefix) {
		m_prefix = prefix;
	}
	
	public void init() {
		
	}

	public void mergeExtensions(String targetUri, String sourceUri) {
		// TODO Auto-generated method stub
		
	}
}
