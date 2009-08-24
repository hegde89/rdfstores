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
package edu.unika.aifb.facetedSearch.api.model.impl;

import edu.unika.aifb.facetedSearch.api.model.IBrowsingObject;
import edu.unika.aifb.facetedSearch.facets.model.impl.StaticFacetValueClusterLeave;
import edu.unika.aifb.facetedSearch.search.session.SearchSession;

/**
 * @author andi
 *
 */
public class BrowsingObject extends AbstractObject implements IBrowsingObject {

	private String m_sourceExtension;
	
	
	protected BrowsingObject(SearchSession session, String value, String extension) {
		super(session, value, extension);
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.api.model.IBrowsingObject#getLeave()
	 */
	public StaticFacetValueClusterLeave getLeave() {
		return super.getSession().getFacetTreeDelegator().getFacetTree(super.getExtension()).;
	}

	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.api.model.IBrowsingObject#getSourceExtension()
	 */
	public String getSourceExtension() {
		return m_sourceExtension;
	}

	/* (non-Javadoc)
	 * @see edu.unika.aifb.facetedSearch.api.model.IBrowsingObject#setSourceExtension()
	 */
	public void setSourceExtension(String extension) {
		m_sourceExtension = extension;
	}
}
