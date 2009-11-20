package edu.unika.aifb.graphindex.util;

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

public class StringSplitter {
	private String m_input;
	private String m_delim;
	private int m_idx = 0;
	
	public StringSplitter(String input, String delim) {
		m_input = input;
		m_delim = delim;
	}
	
	public String next() {
		if (m_idx >= m_input.length())
			return null;
		
		int end = m_input.indexOf(m_delim, m_idx);
		
		if (end < 0 && m_idx < m_input.length() - 1)
			end = m_input.length();
		
		if (end < 0)
			return null;
//		System.out.println(m_idx + " " + end);
		String sub = m_input.substring(m_idx, end);
		m_idx = end + 1;
		
		return sub;
	}
	
	public static void main(String[] args) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 5000000; i++) {
			int length = (int)(Math.random() * 10);
			for (int j = 0; j < length; j++) {
//				System.out.println((int)(Math.random() * 26 + 65));
				sb.append(Character.toString((char)(Math.random() * 26 + 65)));
			}
			sb.append("\n");
		}
		
		
//		String s = "asdasd\nasda\nsdasd\nasdasgggdasd\n";
		String s = sb.toString();
//		System.out.println(s.replaceAll("\n", "\\\\n"));
		StringSplitter split = new StringSplitter(s, "\n");
		
		long start = System.currentTimeMillis();
		String d;
		int splits = 0;
		while ((d = split.next()) != null) {
//			System.out.println(d);
			splits++;
		}
		System.out.println(splits);
		System.out.println(System.currentTimeMillis() - start);
		
		start = System.currentTimeMillis();
		String[] t = s.split("\n");
		System.out.println(t.length);
		System.out.println(System.currentTimeMillis() - start);
		
		
	}
}
