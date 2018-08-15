package com.ge.crawler

import java.util.Collection
import java.util.List

import groovy.json.JsonSlurper

/** 
 * Class responsible for finding and storing the valid links 
 * from JSON files.
 * 
 * @author arunag
 *
 */
class JSONCrawler {
	static String HOME="http://foo.bar.com/p1"

	static String FILE1 = "file/Internet1.json";
	static String FILE2 = "file/Internet2.json";
	static String FILE3 = "file/Internet3.json";
	static String DEFAULT_ENCODING = "UTF-8"
	static String PAGES = "pages"
	static String ADDRESS = "address"
	static String LINKS = "links"
	
	public static void main (String [] s) {
		
		def jsonSlurper = new JsonSlurper()
		File file = new File(JSONCrawler.FILE1)
		def json = jsonSlurper.parseFile(file, JSONCrawler.DEFAULT_ENCODING)
		List pages = json[JSONCrawler.PAGES].collect()
		JSONCrawler.buildOutputLinks(pages)

		println "\n"
		
		jsonSlurper = new JsonSlurper()
		file = new File(JSONCrawler.FILE2)
		json = jsonSlurper.parseFile(file, JSONCrawler.DEFAULT_ENCODING)
		pages = json[JSONCrawler.PAGES].collect()
		JSONCrawler.buildOutputLinks(pages)

		println "\n"
		
		jsonSlurper = new JsonSlurper()
		file = new File(JSONCrawler.FILE3)
		json = jsonSlurper.parseFile(file, JSONCrawler.DEFAULT_ENCODING)
		pages = json[JSONCrawler.PAGES].collect()
		JSONCrawler.buildOutputLinks(pages)

	}
	
	/*
	 *
	 * @param pages
	 */
	static void buildOutputLinks(Collection pages) {
		
		List allLinks = []
		List success = []
		Set skipped = []
		Set errorUrls = []

		def parents=pages[JSONCrawler.ADDRESS].collect()
		def children=pages[JSONCrawler.LINKS].collect()
		
		allLinks.addAll(parents)
		allLinks.addAll(children.flatten())
		
		def buildList
		buildList = { String startUrl ->
			
			def startUrlIndex=parents.indexOf(startUrl)
			def linksToCrawl = children[startUrlIndex].collect().flatten();
			
			linksToCrawl.eachWithIndex{ String link, idx ->
				
				if (success.contains(link) ) {
					//already visited
					skipped.add(link)
					return
				}
				if (!parents.contains(link)) {
					//Link does not have a parent 
					errorUrls.add(link)
					return
				}
				
//				linkList.add(link)
				success.add(link)
				buildList(link)
				
			}
			return allLinks
		}
		
		def startAddress = HOME
		success.add(startAddress)
		
		buildList = buildList.trampoline()
		buildList(startAddress)
		
		println "Success=$success"
		//Find missing urls 
		List missedLink = JSONCrawler.findMissingUrls(allLinks, success)
		errorUrls.addAll(missedLink)
		println "Skipped=$skipped"
		println "Error=$errorUrls"
		
	} // end method
	
	/**
	 * Find the URLs that are never reached.
	 * 
	 * @param allLinks
	 * @param successLinks
	 * @return
	 */
	static List findMissingUrls(List allLinks, List successLinks) {
		List diff = [] 
		if (allLinks.size() > successLinks.size()) {
			 diff = allLinks.minus(successLinks)
		 }
//		 println "Diff=$diff"
		 return diff.collect().flatten()
	}
	
	/** 
	 * Check if the link exist as a parent 
	 * 
	 * @param link
	 * @param parents
	 * @return
	 */
	static boolean isParent(String link, List parents) {
		
		boolean isParent = false
		
		if (parents.contains(link)) {
			isParent=true
		}
		return true;
	}
}
