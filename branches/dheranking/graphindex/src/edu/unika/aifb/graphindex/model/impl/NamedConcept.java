package edu.unika.aifb.graphindex.model.impl;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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

import edu.unika.aifb.graphindex.model.INamedConcept;
import edu.unika.aifb.graphindex.searcher.keyword.model.StructureGraphUtil;

public class NamedConcept implements INamedConcept {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7755683040685980023L;
	
	private String uri;
	private String extension;
	public static NamedConcept TOP = new NamedConcept("http://www.w3.org/2002/07/owl#Thing");
	
	public NamedConcept(String uri)	{
		this.uri = uri;
	}
	
	public NamedConcept(String uri, String extension)	{
		this.uri = uri;
		this.extension = extension;
	}

	public String getLabel() {
		return StructureGraphUtil.getLocalName(uri);
	}

	public String getUri() {
		return uri;
	}
	
	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	public String getExtension() {
		return extension;
	}

	public String toString() {
		return uri; 
	}
	
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((uri == null) ? 0 : uri.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NamedConcept other = (NamedConcept) obj;
		if (uri == null) {
			if (other.uri != null)
				return false;
		} else if (!uri.equals(other.uri))
			return false;
		return true;
	}
	
}
