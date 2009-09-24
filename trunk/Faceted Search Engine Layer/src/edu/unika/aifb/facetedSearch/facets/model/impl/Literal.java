/** 
 * Copyright (C) 2009 Andreas Wagner (andreas.josef.wagner@googlemail.com) 
 *  
 * This file is part of the Faceted Search Layer Project. 
 * 
 * Faceted Search Layer Project is free software: you can redistribute
 * it and/or modify it under the terms of the GNU General Public License, 
 * version 2 as published by the Free Software Foundation. 
 *  
 * Faceted Search Layer Project is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 * GNU General Public License for more details. 
 *  
 * You should have received a copy of the GNU General Public License 
 * along with Faceted Search Layer Project.  If not, see <http://www.gnu.org/licenses/>. 
 */
package edu.unika.aifb.facetedSearch.facets.model.impl;

import java.io.Serializable;

import javax.xml.datatype.XMLGregorianCalendar;

import edu.unika.aifb.facetedSearch.facets.model.ILiteral;

/**
 * @author andi
 * 
 */
public class Literal extends FacetValue
		implements
			ILiteral,
			Comparable<Literal>,
			Serializable {

	private static final long serialVersionUID = -5358821757244268737L;

	private String m_value;
	private Object m_parsedLiteral;

	public Literal() {

	}

	public Literal(String value, Object parsedLit) {

		m_value = value;
		m_parsedLiteral = parsedLit;

	}

	public int compareTo(Literal o) {

		if ((m_parsedLiteral instanceof String)
				&& (o.getParsedLiteral() instanceof String)) {

			return ((String) m_parsedLiteral).compareTo((String) o
					.getParsedLiteral());

		} else if ((m_parsedLiteral instanceof Double)
				&& (o.getParsedLiteral() instanceof Double)) {

			return ((Double) m_parsedLiteral).compareTo((Double) o
					.getParsedLiteral());

		} else if ((m_parsedLiteral instanceof XMLGregorianCalendar)
				&& (o.getParsedLiteral() instanceof XMLGregorianCalendar)) {

			return ((XMLGregorianCalendar) m_parsedLiteral)
					.toGregorianCalendar().compareTo(
							((XMLGregorianCalendar) o.getParsedLiteral())
									.toGregorianCalendar());
		}

		return 0;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof Literal) {
			return m_value.equals(((Literal) obj).getValue());
		} else {
			return false;
		}
	}

	public Object getParsedLiteral() {
		return m_parsedLiteral;
	}

	public void setParsedLiteral(Object parsedLiteral) {
		m_parsedLiteral = parsedLiteral;
	}

	@Override
	public String toString() {
		return m_value;
	}
}
