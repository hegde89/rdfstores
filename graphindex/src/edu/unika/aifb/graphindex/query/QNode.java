package edu.unika.aifb.graphindex.query;

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

import edu.unika.aifb.graphindex.util.Util;

public class QNode implements Comparable<QNode>,Cloneable {
	private String m_label;
	private boolean m_isSelectVariable;
	
	public QNode(String label) {
		m_label = label;
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public boolean isVariable() {
		return Util.isVariable(getLabel());
	}
	
	public boolean isConstant() {
		return Util.isConstant(getLabel());
	}

	public int compareTo(QNode o) {
		return m_label.compareTo(o.getLabel());
	}

	public void setSelectVariable(boolean b) {
		m_isSelectVariable = b;
	}
	
	public boolean isSelectVariable() {
		return m_isSelectVariable;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QNode other = (QNode)obj;
		if (m_label == null) {
			if (other.m_label != null)
				return false;
		} else if (!m_label.equals(other.m_label))
			return false;
		return true;
	}
	
	public String toString() {
		return m_label + (isSelectVariable() ? "*" : "");
	}

	public Object clone() {
		QNode node = new QNode(getLabel());
		node.setSelectVariable(isSelectVariable());
		return node;
	}
}
