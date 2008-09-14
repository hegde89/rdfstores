package edu.unika.aifb.graphindex.importer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ComponentImporter extends Importer {

	@Override
	public void doImport() {
		if (m_files.size() > 1) {
			throw new UnsupportedOperationException("ComponentImporter should have only one input file, the prefix.");
		}
		
		String prefix = m_files.get(0);
		File prefixDir = new File(prefix).getParentFile();
		String namePrefix = new File(prefix).getName();
		String hashesFile = prefix + ".hashes";
		String ntFile = prefix + ".nt";
		
		List<String> componentFiles = new ArrayList<String>();
		for (File file : prefixDir.listFiles())
			if (file.getName().startsWith(namePrefix + ".component"))
				componentFiles.add(file.getAbsolutePath());
		
		NTriplesImporter nt = new NTriplesImporter();
		nt.addImport(ntFile);
	}
}
