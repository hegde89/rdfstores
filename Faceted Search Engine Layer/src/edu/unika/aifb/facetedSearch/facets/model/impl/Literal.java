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

import javax.xml.datatype.XMLGregorianCalendar;

import edu.unika.aifb.facetedSearch.facets.model.ILiteral;

/**
 * @author andi
 * 
 */
public class Literal extends AbstractSingleFacetValue
		implements
			ILiteral,
			Comparable<Literal> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -410835979973111667L;

	/*
	 * 
	 */
	private Object m_parsedLiteral;

	public Literal() {

	}
	public Literal(String value, Object parsedLit) {

		super(value);
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

	public Object getParsedLiteral() {
		return m_parsedLiteral;
	}


	public void setParsedLiteral(Object parsedLiteral) {
		m_parsedLiteral = parsedLiteral;
	}
}
