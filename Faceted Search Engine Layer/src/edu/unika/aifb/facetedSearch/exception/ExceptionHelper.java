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
package edu.unika.aifb.facetedSearch.exception;

/**
 * @author andi
 * 
 */
public class ExceptionHelper {

	public static class Cause {

		public static int NOT_VALID = 0;
		public static int MISSING = 1;
	}

	public static String createMessage(String property, int cause) {

		String msg = "";

		if (cause == Cause.NOT_VALID) {
			msg += property + " value is not valid. Please check!";
		} else if (cause == Cause.MISSING) {
			msg += property + " is missing. Please check!";
		}

		return msg;
	}

}
