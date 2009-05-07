package edu.unika.aifb.graphindex.storage;

public class StorageException extends Exception {

	private static final long serialVersionUID = 4906803688020527407L;

	public StorageException() {
	}

	public StorageException(String message) {
		super(message);
	}

	public StorageException(Throwable cause) {
		super(cause);
	}

	public StorageException(String message, Throwable cause) {
		super(message, cause);
	}
}
