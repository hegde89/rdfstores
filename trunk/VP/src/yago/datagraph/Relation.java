package yago.datagraph;
import java.lang.reflect.*;
import java.util.*;

import yago.javatools.*;

/** 
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 * 
 * This class represents a YAGO relation.<P>
 *
 * If a property of relations is added, all methods in the class Relation.java need to be adjusted!
 * RelationExtractor.java does not need to be adjusted.
 * Relations are represented as Java-objects in order to allow other Generators/Extractors to
 * access the information about relations.
 * 
 *  @author Fabian M. Suchanek*/
public class Relation implements Comparable<Relation> {
	public static final Relation MEANS=new Relation("means",YagoClass.WORD,YagoClass.ENTITY,false,false);
  public static final Relation ISCALLED=new Relation("isCalled",YagoClass.ENTITY,YagoClass.WORD,false,false);
	public static final Relation TYPE=new Relation("type",YagoClass.ENTITY,YagoClass.CLASS,false,false);
	public static final Relation SUBCLASSOF=new Relation("subClassOf",YagoClass.CLASS,YagoClass.CLASS,true,false);
	public static final Relation DOMAIN=new Relation("domain",YagoClass.RELATION,YagoClass.CLASS,false,false);    
	public static final Relation RANGE=new Relation("range",YagoClass.RELATION,YagoClass.CLASS,false,false);    
	public static final Relation SUBPROPERTYOF=new Relation("subPropertyOf",YagoClass.RELATION,YagoClass.RELATION,true,false);	  

	public static final Relation FAMILYNAME=new Relation("familyNameOf",YagoClass.WORD,YagoClass.PERSON,true,false,MEANS);   
	public static final Relation GIVENNAME=new Relation("givenNameOf",YagoClass.WORD,YagoClass.PERSON,true,false,MEANS);   
	public static final Relation DESCRIBES=new Relation("describes",YagoClass.URL,YagoClass.ENTITY,false,false);
	public static final Relation ESTABLISHEDONDATE=new Relation("establishedOnDate",YagoClass.ENTITY,YagoClass.TIMEINTERVAL,false,false);
	public static final Relation HASWONPRIZE=new Relation("hasWonPrize",YagoClass.ENTITY,YagoClass.PRIZE,false,false);
	public static final Relation WRITTENINYEAR=new Relation("writtenInYear",YagoClass.ENTITY,YagoClass.DATE,false,true);
	public static final Relation LOCATEDIN=new Relation("locatedIn",YagoClass.ENTITY,YagoClass.ENTITY,true,false);
	public static final Relation POLITICIANOF=new Relation("politicianOf",YagoClass.PERSON,YagoClass.GEOPOLITICAL,false,false);
	public static final Relation CONTEXT=new Relation("context",YagoClass.ENTITY,YagoClass.ENTITY,false,false);    
  public static final Relation ISCITIZENOF=new Relation("isCitizenOf",YagoClass.PERSON,YagoClass.COUNTRY,false,false);
  public static final Relation MEREONYMY=new Relation("isMereonymOf",YagoClass.CLASS,YagoClass.CLASS,false,false);
  public static final Relation MEMBEROF=new Relation("isMemberOf",YagoClass.CLASS,YagoClass.CLASS,false,false, MEREONYMY);
  public static final Relation SUBSTANCEOF=new Relation("isSubstanceOf",YagoClass.CLASS,YagoClass.CLASS,false,false, MEREONYMY);
  public static final Relation PARTOF=new Relation("isPartOf",YagoClass.CLASS,YagoClass.CLASS,false,false, MEREONYMY);
  public static final Relation FOUNDIN=new Relation("foundIn",YagoClass.FACT,YagoClass.URL,false,false);
  public static final Relation USING=new Relation("using",YagoClass.FACT,YagoClass.COMPUTERSYSTEM,false,false);
  public static final Relation DURING=new Relation("during",YagoClass.FACT,YagoClass.TIMEINTERVAL,false,true);  
  public static final Relation SINCE=new Relation("since",YagoClass.FACT,YagoClass.TIMEINTERVAL,false,false);
  public static final Relation UNTIL=new Relation("until",YagoClass.FACT,YagoClass.TIMEINTERVAL,false,false);
  public static final Relation INLANGUAGE=new Relation("inLanguage",YagoClass.FACT,YagoClass.LANGUAGE,false,false);
  public static final Relation HASVALUE=new Relation("hasValue",YagoClass.QUANTITY,YagoClass.NUMBER,false,false);
  public static final Relation INUNIT=new Relation("inUnit",YagoClass.FACT,YagoClass.UNIT,false,true);
  
  // FMS: Data concerning Inventions, by Gjergji
  public static final Relation DISCOVERED=new Relation("discovered",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation DISCOVEREDONDATE=new Relation("discoveredOnDate",YagoClass.ENTITY,YagoClass.TIMEINTERVAL,false,true);      
  
	/*// MR: IMDB Data
	public static final Relation _HASALTERNATETITLE = new Relation ("_hasAlternateTitle",YagoClass.ENTITY,YagoClass.ENTITY,true,false,MEANS); 	
	public static final Relation _PRODUCTIONLANGUAGE = new Relation ("_hasProductionLanguage",YagoClass.ENTITY,YagoClass.ENTITY,false,false);	
	public static final Relation _PRODUCTIONCOUNTRY = new Relation ("_producedInCountry",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
	public static final Relation _ISOFTYPE = new Relation ("_isOfType",YagoClass.ENTITY,YagoClass.ENTITY,false,false);

	public static final Relation _SEX = new Relation ("_isOfSex",YagoClass.PERSON,YagoClass.ENTITY,false,true);
	public static final Relation _GUESTAPPEARANCE = new Relation ("_guestIn",YagoClass.PERSON,YagoClass.ENTITY,false,false);
	public static final Relation _REALNAME = new Relation ("_hasRealName",YagoClass.PERSON,YagoClass.ENTITY,false,false,MEANS);
	public static final Relation _NICKNAME = new Relation ("_hasNickName",YagoClass.PERSON,YagoClass.ENTITY,false,false,MEANS);
	public static final Relation _AKA = new Relation ("_alsoKnownAs",YagoClass.PERSON,YagoClass.ENTITY,false,true,MEANS);
	public static final Relation _HEIGHT = new Relation ("_hasHeight",YagoClass.PERSON,YagoClass.RATIONAL,false,true);
	public static final Relation _CAUSEOFDEATH = new Relation ("_causeOfDeath",YagoClass.PERSON,YagoClass.ENTITY,false,false);
	public static final Relation _ROLE = new Relation ("_playedRole",YagoClass.PERSON,YagoClass.RELATION,false,false);
*/
  // Template facts
  public static final Relation BORNONDATE = new Relation ("bornOnDate",YagoClass.PERSON,YagoClass.TIMEINTERVAL,false,true);
  public static final Relation BORNIN = new Relation ("bornIn",YagoClass.PERSON,YagoClass.GEOPOLITICAL,false,true);
  public static final Relation ORIGIN = new Relation ("originatesFrom",YagoClass.ENTITY,YagoClass.GEOPOLITICAL,false,true);
  public static final Relation DIEDONDATE = new Relation ("diedOnDate",YagoClass.PERSON,YagoClass.TIMEINTERVAL,false,true);
  public static final Relation DIEDIN = new Relation ("diedIn",YagoClass.PERSON,YagoClass.GEOPOLITICAL,false,true); // People die in all kinds of hospitals, churches etc.
  public static final Relation NATIVENAME= new Relation ("isNativeNameOf",YagoClass.WORD,YagoClass.ENTITY,false,false,Relation.MEANS);
  public static final Relation LEADER= new Relation ("isLeaderOf",YagoClass.PERSON,YagoClass.ENTITY,false,false);  
  public static final Relation AREA= new Relation ("hasArea",YagoClass.GEOPOLITICAL,YagoClass.AREA,false,true);
  public static final Relation POPULATION= new Relation ("hasPopulation",YagoClass.GEOPOLITICAL,YagoClass.NONNEGATIVEINTEGER,false,true);
  public static final Relation POPULATIONDENSITY= new Relation ("hasPopulationDensity",YagoClass.GEOPOLITICAL,YagoClass.DENSITYPERAREA,false,true);
  public static final Relation UTCOFFSET= new Relation ("hasUTCOffset",YagoClass.GEOPOLITICAL,YagoClass.INTEGER,false,false);
  public static final Relation WEBSITE= new Relation ("hasWebsite",YagoClass.ENTITY,YagoClass.URL,false,false);
  public static final Relation ORDER= new Relation ("isNumber",YagoClass.ENTITY,YagoClass.NONNEGATIVEINTEGER,false,true);
  public static final Relation PREDECESSOR= new Relation ("hasPredecessor",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation SUCCESSOR= new Relation ("hasSuccessor",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation MARRIEDTO= new Relation ("isMarriedTo",YagoClass.PERSON,YagoClass.PERSON,false,false);
  public static final Relation AFFILIATEDTO= new Relation ("isAffiliatedTo",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation INFLUENCED= new Relation ("influences",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation DIRECTED = new Relation ("directed",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation PRODUCED = new Relation ("produced",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation EDITED = new Relation ("edited",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation ACTEDIN = new Relation ("actedIn",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation PUBLISHEDONDATE = new Relation ("publishedOnDate",YagoClass.ENTITY,YagoClass.TIMEINTERVAL,false,true);
  public static final Relation DURATION= new Relation ("hasDuration",YagoClass.ENTITY,YagoClass.DURATION,false,true);
  public static final Relation PRODUCTIONLANGUAGE = new Relation ("hasProductionLanguage",YagoClass.ENTITY,YagoClass.LANGUAGE,false,false);
  public static final Relation BUDGET= new Relation ("hasBudget",YagoClass.ENTITY,YagoClass.MONETARYVALUE,false,true);
  public static final Relation IMDB= new Relation ("hasImdb",YagoClass.MOVIE,YagoClass.IDENTIFIER,false,true);
  public static final Relation PRODUCEDIN = new Relation ("producedIn",YagoClass.ENTITY,YagoClass.COUNTRY,false,false);
  public static final Relation CHILD= new Relation ("hasChild",YagoClass.PERSON,YagoClass.PERSON,false,false);
  public static final Relation MOTTO= new Relation ("hasMotto",YagoClass.ENTITY,YagoClass.STRING,false,false);
  public static final Relation OFFICIALLANGUAGE= new Relation ("hasOfficialLanguage",YagoClass.GEOPOLITICAL,YagoClass.LANGUAGE,false,false);
  public static final Relation CAPITAL= new Relation ("hasCapital",YagoClass.GEOPOLITICAL,YagoClass.GEOPOLITICAL,false,true); // A "capital" is not a city in WordNet...
  public static final Relation WATER= new Relation ("hasWaterPart",YagoClass.GEOPOLITICAL,YagoClass.PROPORTION,false,true);
  public static final Relation GDP= new Relation ("hasGDPPPP",YagoClass.GEOPOLITICAL,YagoClass.MONETARYVALUE,false,true);  
  public static final Relation GDPN= new Relation ("hasNominalGDP",YagoClass.GEOPOLITICAL,YagoClass.MONETARYVALUE,false,true);
  public static final Relation HDI= new Relation ("hasHDI",YagoClass.GEOPOLITICAL,YagoClass.RATIONAL,false,true);
  public static final Relation GINI= new Relation ("hasGini",YagoClass.GEOPOLITICAL,YagoClass.RATIONAL,false,true);
  public static final Relation CURRENCY= new Relation ("hasCurrency",YagoClass.GEOPOLITICAL,YagoClass.ENTITY,false,true);
  public static final Relation TIMEZONE= new Relation ("inTimeZone",YagoClass.GEOPOLITICAL,YagoClass.ENTITY,false,false);
  public static final Relation TOPLEVELDOMAIN= new Relation ("hasTLD",YagoClass.GEOPOLITICAL,YagoClass.TLD,false,false);
  public static final Relation CALLINGCODE= new Relation ("hasCallingCode",YagoClass.GEOPOLITICAL,YagoClass.CALLINGCODE,false,false);
  public static final Relation WROTE= new Relation ("wrote",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation MADECOVER= new Relation ("madeCoverFor",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation GENRE = new Relation ("isOfGenre",YagoClass.ENTITY,YagoClass.CLASS,false,false);
  public static final Relation PUBLISHED= new Relation ("published",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation PAGES= new Relation ("hasPages",YagoClass.ENTITY,YagoClass.NONNEGATIVEINTEGER,false,true);
  public static final Relation ISBN= new Relation ("hasISBN",YagoClass.ENTITY,YagoClass.ISBN,false,true);
  public static final Relation JOINED= new Relation ("joined",YagoClass.GEOPOLITICAL,YagoClass.GEOPOLITICAL,false,true);
  public static final Relation LIVESIN= new Relation ("livesIn",YagoClass.PERSON,YagoClass.GEOPOLITICAL,false,false);
  public static final Relation HEIGHT= new Relation ("hasHeight",YagoClass.ENTITY,YagoClass.LENGTH,false,true);
  public static final Relation WEIGHT= new Relation ("hasWeight",YagoClass.ENTITY,YagoClass.WEIGHT,false,true);
  public static final Relation SPOKENIN= new Relation ("isSpokenIn",YagoClass.LANGUAGE,YagoClass.GEOPOLITICAL,false,false);
  public static final Relation CREATED= new Relation ("created",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation CREATEDONDATE= new Relation ("createdOnDate",YagoClass.ENTITY,YagoClass.TIMEINTERVAL,false,false);
  public static final Relation INTERESTEDIN= new Relation ("interestedIn",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation GROWTH= new Relation ("hasEconomicGrowth",YagoClass.GEOPOLITICAL,YagoClass.PROPORTION,false,true);
  public static final Relation INFLATION= new Relation ("hasInflation",YagoClass.GEOPOLITICAL,YagoClass.PROPORTION,false,true);
  public static final Relation POVERTY= new Relation ("hasPoverty",YagoClass.GEOPOLITICAL,YagoClass.PROPORTION,false,true);
  public static final Relation LABOR= new Relation ("hasLabor",YagoClass.GEOPOLITICAL,YagoClass.NONNEGATIVEINTEGER,false,true);
  public static final Relation UNEMPLOYMENT= new Relation ("hasUnemployment",YagoClass.GEOPOLITICAL,YagoClass.PROPORTION,false,true);
  public static final Relation EXPORTVALUE= new Relation ("hasExport",YagoClass.GEOPOLITICAL,YagoClass.MONETARYVALUE,false,true);
  public static final Relation EXPORTS= new Relation ("exports",YagoClass.GEOPOLITICAL,YagoClass.CLASS,false,false);
  public static final Relation IMPORTVALUE= new Relation ("hasImport",YagoClass.GEOPOLITICAL,YagoClass.MONETARYVALUE,false,true);
  public static final Relation IMPORTS= new Relation ("imports",YagoClass.GEOPOLITICAL,YagoClass.CLASS,false,false);
  public static final Relation DEALSWITH= new Relation ("dealsWith",YagoClass.GEOPOLITICAL,YagoClass.GEOPOLITICAL,false,false);  
  public static final Relation REVENUE= new Relation ("hasRevenue",YagoClass.ENTITY,YagoClass.MONETARYVALUE,false,true);
  public static final Relation EXPENSES= new Relation ("hasExpenses",YagoClass.ENTITY,YagoClass.MONETARYVALUE,false,true);  
  public static final Relation MILITARY= new Relation ("hasMilitary",YagoClass.GEOPOLITICAL,YagoClass.ENTITY,false,false);
  public static final Relation NUMBEROFPEOPLE= new Relation ("hasNumberOfPeople",YagoClass.ENTITY,YagoClass.NONNEGATIVEINTEGER,false,true);
  public static final Relation USESGDP= new Relation ("usesGDP",YagoClass.ENTITY,YagoClass.PROPORTION,false,true);
  public static final Relation WORKSAT= new Relation ("worksAt",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation ALMAMATER= new Relation ("graduatedFrom",YagoClass.PERSON,YagoClass.ENTITY,false,false);
  public static final Relation ACADEMICADVISOR= new Relation ("hasAcademicAdvisor",YagoClass.PERSON,YagoClass.PERSON,false,false);
  public static final Relation HAPPENEDONDATE= new Relation ("happenedOnDate",YagoClass.ENTITY,YagoClass.TIMEINTERVAL,false,false);
  public static final Relation HAPPENEDIN= new Relation ("happenedIn",YagoClass.ENTITY,YagoClass.GEOPOLITICAL,false,false);
  public static final Relation PARTICIPATEDIN= new Relation ("participatedIn",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  public static final Relation MUSICALROLE= new Relation ("musicalRole",YagoClass.PERSON,YagoClass.CLASS,false,false);
  public static final Relation HASPRODUCT= new Relation ("hasProduct",YagoClass.ENTITY,YagoClass.ENTITY,false,false);
  
  /** Holds the name of this relation*/
	public String name;
  /** Holds the domain of this relation*/ 
	public YagoClass domain=null;
  /** Holds the range of this relation*/
	public YagoClass range=null;
  /** Points to the super relation (or null)*/
	public Relation superRelation;
  /** Tells whether this relation is transitive*/
	public boolean isTransitive; 
  /** Tells whether this relation is right-unique*/
  public boolean isFunction; 
  
	/** Constructor with all properties*/
  public Relation(String name, YagoClass domain, YagoClass range, boolean isTransitive, boolean isFunction) {
    this(name,domain,range,isTransitive,isFunction,null);
  }

	/** Constructor with all properties and type*/
	public Relation(String name, YagoClass domain, YagoClass range, boolean isTransitive, boolean isFunction, Relation superRelation) {
    this.name=name;
    this.domain=domain;
    this.range=range;    
    this.isTransitive=isTransitive;
    this.isFunction=isFunction;
		this.superRelation=superRelation;
	}    

	/** Constructor with name */
	protected Relation(String name) {
    this(name,YagoClass.ENTITY, YagoClass.ENTITY, false,false);
	}

	/** Returns a line for the relationships table*/
	public String tableLine() {
		return(name+'\t'+domain+'\t'+range+'\t'+isTransitive+'\t'+isFunction);
	}

	/** All relations that apply to relations */
	public static Relation[] propertyList=new Relation[]{DOMAIN,RANGE,SUBPROPERTYOF,TYPE};

	/** Maps a relation of propertyList to the appropriate value*/
	public Object getProperty(Relation prop) {
		if(prop==DOMAIN) return(domain);
		if(prop==RANGE) return(range);
		if(prop==SUBPROPERTYOF) return(superRelation==null?null:superRelation);
		if(prop==TYPE) {
			if(isTransitive) return(YagoClass.TRANSITIVERELATION);
			if(isFunction) return(YagoClass.FUNCTION);
			return(YagoClass.RELATION);
		}
		return(null);
	}

	/** Returns the super-Relation*/
	public Relation getSuperRelation() {
		return(superRelation);      
	}

	/** Returns the relation name */
	public String toString() {
		return(name());
	}

	/** Returns the relation name */
	public String name() {
		return(name);
	}

	/** Returns all super relations of this relation (basing on its properties)*/
	public List<YagoClass> getSuper() {
		List<YagoClass> l=new ArrayList<YagoClass>(2);
		if(isTransitive) l.add(YagoClass.TRANSITIVERELATION);
		if(isFunction) l.add(YagoClass.FUNCTION);
		if(l.size()==0) l.add(YagoClass.RELATION);
		return(l);
	}
  
  /** Tells whether two relations are the same (by id)*/

	public boolean equals(Object obj) {     
		return(obj==this);
	}
  

	/** List of all non-virtual relations declared in this class*/
	public static List<Relation> values=new ArrayList<Relation>(20);
	static {
		for(Field f : Relation.class.getFields()) {      
			if(!f.getDeclaringClass().equals(yago.datagraph.Relation.class)) continue;
			try {
				values.add((Relation)f.get(null));
			}
			catch(Exception e) {
				// This won't happen, because we're inside the Relation class
			}          
		}
	}
  
	/** List of all non-virtual relations declared in this class*/    
	public static List<Relation> values() {
		return(values);
	}
  
  /** Returns the relation for a given name (if the relation is declared)*/

	/** Maps a String to a Relation (or to null)*/
	public static Relation valueOf(String s) {
		for(Relation r : values()) {
			if(r.name().equalsIgnoreCase(s)) return(r);
		}
		return(null);
	}
  
  /** Compares two relations by name*/
	public int compareTo(Relation o) {      
		return(name.compareTo(o.name()));
	}
  
  /** Returns the hash of the name*/
	public int hashCode() {    
		return name.hashCode();
	}
  
}
