package edu.unika.aifb.graphindex.importer;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.LinkedList;
import java.util.List;

public class CompositeImporter extends Importer {

	private List<Importer> m_importers;
	
	public CompositeImporter() {
		m_importers = new LinkedList<Importer>();
	}
	
	public void addImporter(Importer importer) {
		m_importers.add(importer);
	}
	
	@Override
	public void doImport() {
		for (Importer importer : m_importers) {
			importer.setTripleSink(m_sink);
			importer.doImport();
		}
	}

}
