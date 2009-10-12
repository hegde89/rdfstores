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
import edu.unika.aifb.facetedSearch.util.FacetUtils;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author andi
 * 
 */
public class Literal extends AbstractSingleFacetValue implements ILiteral {

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

	@Override
	public int compareTo(AbstractBrowsingObject obj) {

		if (obj instanceof Literal) {

			Literal lit = (Literal) obj;

			if ((m_parsedLiteral instanceof String)
					&& (lit.getParsedLiteral() instanceof String)) {

				return ((String) m_parsedLiteral).compareTo((String) lit
						.getParsedLiteral());

			} else if ((m_parsedLiteral instanceof Double)
					&& (lit.getParsedLiteral() instanceof Double)) {

				return ((Double) m_parsedLiteral).compareTo((Double) lit
						.getParsedLiteral());

			} else if ((m_parsedLiteral instanceof XMLGregorianCalendar)
					&& (lit.getParsedLiteral() instanceof XMLGregorianCalendar)) {

				return ((XMLGregorianCalendar) m_parsedLiteral)
						.toGregorianCalendar().compareTo(
								((XMLGregorianCalendar) lit.getParsedLiteral())
										.toGregorianCalendar());
			}
		}

		return 0;
	}

	public Object getParsedLiteral() {
		return m_parsedLiteral;
	}

	public void setParsedLiteral(Object parsedLiteral) {
		m_parsedLiteral = parsedLiteral;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * edu.unika.aifb.facetedSearch.facets.model.impl.AbstractBrowsingObject
	 * #toString()
	 */
	@Override
	public String toString() {

		if (super.getValue() != null) {
			return Util.truncateUri(FacetUtils.getValueOfLiteral(
					super.getValue()).toLowerCase())
					+ " (" + super.getCountS() + ")";
		} else {
			return null;
		}
	}
}
