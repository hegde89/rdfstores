package edu.unika.aifb.vponmonet;


public class ImportException extends Exception {

	private static final long serialVersionUID = -2051783434396941188L;

	public ImportException(String string) {
		super(string);
	}

	public ImportException(Throwable e) {
		super(e);
	}

}
