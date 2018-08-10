package com.ge

import groovy.json.JsonSlurper

class ParseJSON {
	static void main(String [] s) {
		def jsonSlurper = new JsonSlurper()
		
		File file = new File("file/Internet1.json")
		def object = jsonSlurper.parse(file);
		
assert object instanceof Map
	}
}
