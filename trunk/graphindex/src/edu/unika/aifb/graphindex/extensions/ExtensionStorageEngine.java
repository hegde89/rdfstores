package edu.unika.aifb.graphindex.extensions;

public interface ExtensionStorageEngine {
	public void storeExtension(Extension ext);
	public Extension readExtension(String uri);
	public void removeExtension(String uri);
	public void removeAllExtensions();
	public boolean extensionExists(String uri);
	public int numberOfExtensions();
	public void setPrefix(String prefix);
	public void init();
	public void mergeExtensions(String targetUri, String sourceUri);
}
