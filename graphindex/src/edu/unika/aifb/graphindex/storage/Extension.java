package edu.unika.aifb.graphindex.storage;

import java.util.Set;

public interface Extension {
	public String getUri();
	
	public void unload() throws StorageException;

	public Set<Triple> getTriples(String propertyUri) throws StorageException;
	public Set<Triple> getTriples(String propertyUri, String objectValue) throws StorageException;
	
	public void addTriple(String subject, String property, String object) throws StorageException;
	public void addTriple(Triple triple) throws StorageException;
	public void addTriples(Set<Triple> triples) throws StorageException;

	public void mergeExtension(Extension extension) throws StorageException;
}
