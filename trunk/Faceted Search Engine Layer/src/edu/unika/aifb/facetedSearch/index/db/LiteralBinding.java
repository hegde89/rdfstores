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
package edu.unika.aifb.facetedSearch.index.db;

import java.io.IOException;

import com.sleepycat.bind.serial.SerialInput;
import com.sleepycat.bind.serial.SerialOutput;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import edu.unika.aifb.facetedSearch.facets.model.impl.Literal;

/**
 * @author andi
 * 
 */
public class LiteralBinding extends TupleBinding<Literal> {

	private StoredClassCatalog m_cata;

	public LiteralBinding(StoredClassCatalog cata) {
		m_cata = cata;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#entryToObject(com.sleepycat.bind
	 * .tuple.TupleInput)
	 */
	@Override
	public Literal entryToObject(TupleInput input) {

		Object parsedLiteral = null;

		try {
			SerialInput serialInput = new SerialInput(input, m_cata);
			parsedLiteral = serialInput.readObject();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		String value = input.readString();
		String ext = input.readString();

		Literal lit = new Literal();
		lit.setParsedLiteral(parsedLiteral);
		lit.setValue(value);
		lit.setSourceExt(ext);

		return lit;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.sleepycat.bind.tuple.TupleBinding#objectToEntry(java.lang.Object,
	 * com.sleepycat.bind.tuple.TupleOutput)
	 */
	@Override
	public void objectToEntry(Literal object, TupleOutput output) {

		try {

			SerialOutput serialOut = new SerialOutput(output, m_cata);
			serialOut.writeObject(object.getParsedLiteral());

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		output.writeString(object.getValue());
		output.writeString(object.getSourceExt());

	}
}
