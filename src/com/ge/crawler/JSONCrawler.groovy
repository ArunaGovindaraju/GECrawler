package com.ge.crawler

import groovy.json.JsonSlurper
import groovyx.gpars.GParsPool

/** 
 * Class same as JSONCrawler
 * This class is an example that uses GPARS.jar for Asynchronous Parallel Collection navigation 
 * 
 * @author arunag
 *
 */
class JSONCrawler {
	
	static String FILE1 = "files/Internet1.json";
	static String FILE2 = "files/Internet2.json";
	static String FILE3 = "files/Internet3.json";
	static String DEFAULT_ENCODING = "UTF-8"
	static String PAGES = "pages"
	static String ADDRESS = "address"
	static String LINKS = "links"
	static String HOME="http://foo.bar.com/p1"
	
	/** 
	 * Main Class that feeds three different input files to 
	 * JSONCraler#buildOutputLinks
	 * @param s
	 */
	public static void main (String [] s) {
		
		
		Collection files = [JSONCrawler.FILE1, JSONCrawler.FILE2, JSONCrawler.FILE3]
		
//		Runs the files in parallel using the GPARS component.
		GParsPool.withPool(3) {
			files.eachParallel{
				println it
				JSONCrawler.crawl(it)
			}
		}
	}
	
	/*
	 * Builds the required Output links such as the "successUrls", skippedUrls, errorUrls 
	 * by iterating through the elements in the Json file.
	 * 
	 * @param pages
	 */
	private static void crawl(String filePath) {
		
		def jsonSlurper = new JsonSlurper()
		File file = new File(filePath)
		def json = jsonSlurper.parseFile(file, JSONCrawler.DEFAULT_ENCODING)
		assert json instanceof Map
		List pages = json[JSONCrawler.PAGES].collect()
		
		List allLinks = []
		List successUrls = []
		Set skippedUrls = []
		Set errorUrls = []

		def parents=pages[JSONCrawler.ADDRESS].collect()
		def children=pages[JSONCrawler.LINKS].collect()
		
		assert parents instanceof Collection
		assert children instanceof Collection
		
		allLinks.addAll(parents)
		allLinks.addAll(children.flatten())
		
		def buildList
		buildList = { String startUrl ->
			
			def startUrlIndex=parents.indexOf(startUrl)
			def linksToCrawl = children[startUrlIndex].collect().flatten();
			
		GParsPool.withPool(3) {
		linksToCrawl.eachWithIndexParallel{ String link, idx ->
					
					if (successUrls.contains(link) ) {
						//return to next idx if already visited
						skippedUrls.add(link)
						return
					}
					if (!parents.contains(link)) {
						/*
						 * Return to next idx if the link is not a parent , since there are no further links to crawl.
						 * Note this is as expected. 
						 * But the parent links must also be indexed but not crawled. This should  be included in "successUrls" list.
						 */
						errorUrls.add(link)
						return
					}
	//				linkList.add(link)
					successUrls.add(link)
					buildList(link)
				}
		}
			return allLinks
		}
		
		def startAddress = HOME
		successUrls.add(startAddress)
		
		buildList = buildList.trampoline()
		buildList(startAddress)

		//Find Urls not index, skipped.
		List missedLink = JSONCrawler.findMissingUrls(allLinks, successUrls)
		errorUrls.addAll(missedLink)

		println "For $filePath :: Success=$successUrls"
		println "For $filePath :: Skipped=$skippedUrls"
		println "For $filePath :: Error=$errorUrls"
		
		assert !successUrls.equals(null)
		assert !skippedUrls.equals(null)
		assert !errorUrls.equals(null)
		
		// Inlined test case
		def validate ={
			if (filePath.equalsIgnoreCase(JSONCrawler.FILE1)) {
				assert successUrls.containsAll(INTERNET1_SUCCESS)
				assert skippedUrls.collect().containsAll(INTERNET1_SKIPPED)
				assert errorUrls.collect().containsAll(INTERNET1_ERROR)
				println "Links from $filePath crawled successfully!"
			} else if (filePath.equalsIgnoreCase(JSONCrawler.FILE2)) {
				assert successUrls.containsAll(INTERNET2_SUCCESS)
				assert skippedUrls.collect().containsAll(INTERNET2_SKIPPED)
				assert errorUrls.collect().containsAll(INTERNET2_ERROR)
				println "Links from $filePath crawled successfully!"
			}
		}
		validate()
		 
	} // end method
	
	/**
	 * Find the URLs that are never reached.
	 * 
	 * @param allLinks
	 * @param successLinks
	 * @return
	 */
	protected static List findMissingUrls(List allLinks, List successLinks) {
		List diff = [] 
		if (allLinks.size() > successLinks.size()) {
			 diff = allLinks.minus(successLinks)
		 }
		 return diff.collect().flatten()
	}
	
	/** 
	 * Check if the link exist as a parent 
	 * 
	 * @param link
	 * @param parents
	 * @return
	 */
	protected static boolean isParent(String link, List parents) {
		boolean isParent = false
		if (parents.contains(link)) {
			isParent=true
		}
		return true;
	}
	
	
//	Internet1 crawl results
	static final INTERNET1_SUCCESS=
		["http://foo.bar.com/p1", "http://foo.bar.com/p2",
		"http://foo.bar.com/p4", "http://foo.bar.com/p5",
		"http://foo.bar.com/p6"]
	
	static final INTERNET1_SKIPPED=
		["http://foo.bar.com/p2",
		"http://foo.bar.com/p4","http://foo.bar.com/p1",
		"http://foo.bar.com/p5"]
	static final INTERNET1_ERROR=
		["http://foo.bar.com/p3", "http://foo.bar.com/p7"]
	
//	Internet2 crawl results	
	static final INTERNET2_SUCCESS=
		["http://foo.bar.com/p1", "http://foo.bar.com/p2",
		"http://foo.bar.com/p3", "http://foo.bar.com/p4",
		"http://foo.bar.com/p5"]
	
	static final INTERNET2_SKIPPED=["http://foo.bar.com/p1"]

		//Object modified to include p6 since this cannot be reached.
	static final INTERNET2_ERROR=["http://foo.bar.com/p6"]
}
