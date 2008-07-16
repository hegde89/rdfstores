package edu.unika.aifb.graphindex.extensions;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * <p>Each extension is saved to one file. The filename is generated from the extension uri.</p>
 * <p>
 * File format:
 * <pre>
 * -&lt;edgeuri&gt;
 * &lt;entryuri&gt;\t&lt;parenturi&gt;\t&lt;parenturi&gt;\t...
 * ...
 * -&lt;edgeuri&gt;
 * ...
 * </pre>
 * </p>
 * 
 * @author gl
 *
 */
public class ExtensionManager {

	private ExtensionStorageEngine m_storage;
	private Map<String,Extension> m_loadedExts;
	private int m_extsRead, m_extsWritten, m_gets;
	
	private static ExtensionManager m_instance;
	
	private ExtensionManager() {
		m_loadedExts = new HashMap<String,Extension>();
	}
	
	public static ExtensionManager getInstance() {
		if (m_instance == null)
			m_instance = new ExtensionManager();
		return m_instance;
	}
	
	public void setStorageEngine(ExtensionStorageEngine storage) {
		m_storage = storage;
	}
	
	public void mergeExtension(String targetUri, String sourceUri) {
		m_storage.mergeExtensions(targetUri, sourceUri);
//		Extension target = getExtension(targetUri);
//		Extension source = getExtension(sourceUri);
//		
//		for (ExtEntry e : source.getEntries()) {
//			if (e.getParents().size() == 0)
//				target.add(e.getUri(), e.getEdgeUri(), null);
//			else 
//				for (String parentUri : e.getParents()) 
//					target.add(e.getUri(), e.getEdgeUri(), parentUri);
//		}
//		
//		removeExtension(sourceUri);
	}
	
	private void removeExtension(String uri) {
		m_storage.removeExtension(uri);
		m_loadedExts.remove(uri);
	}
	
	public void removeAllExtensions() {
		m_storage.removeAllExtensions();
		m_loadedExts.clear();
	}
	
	private void loadExtension(String uri) {
		if (!isExtensionLoaded(uri)) {
			Extension ext;
			if (m_storage.extensionExists(uri)) {
				ext = m_storage.readExtension(uri);
				m_extsRead++;
			}
			else
				ext = new Extension(uri);
			m_loadedExts.put(uri, ext);
		}
	}
	
	public boolean isExtensionLoaded(String uri) {
		return m_loadedExts.containsKey(uri);
	}
	
	public void addExtension(Extension ext) {
		m_loadedExts.put(ext.getUri(), ext);
	}
	
	public Extension getExtension(String uri) {
		m_gets++;
		if (!isExtensionLoaded(uri))
			loadExtension(uri);
		return m_loadedExts.get(uri);
	}
	
	private void unloadExtension(String uri) {
		if (isExtensionLoaded(uri)) {
			m_storage.storeExtension(m_loadedExts.get(uri));
			m_extsWritten++;
			m_loadedExts.remove(uri);
		}
	}
	
	public void unloadAllExtensions() {
		Set<String> uris = new HashSet<String>(m_loadedExts.keySet());
		for (String uri : uris)
			unloadExtension(uri);
		System.gc();
	}

	public String stats() {
		return "extensions: " + m_storage.numberOfExtensions() + ", read: " + m_extsRead + ", written: " + m_extsWritten + ", loaded: " + m_loadedExts.size() + ", gets: " + m_gets;
	}
}
