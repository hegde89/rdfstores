package edu.unika.aifb.graphindex.importer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TriplesImporter extends Importer {

	@Override
	public void doImport() {
		for (String f : m_files) {
			try {
				BufferedReader in = new BufferedReader(new FileReader(f));
				String input;
				while ((input = in.readLine()) != null) {
					input = input.trim();
					String[] t = input.split("\t");
					m_sink.triple(t[0], t[1], t[2]);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
