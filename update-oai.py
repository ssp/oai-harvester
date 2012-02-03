#!/usr/bin/env python
#coding=utf-8
import sys
import os
import getopt
import xml.etree
from xml.etree import ElementTree
from lxml import etree
import subprocess
import urllib2

dataPath = 'data'
downloadScriptPath = 'harvester/OaiList.pl'

def main():
	harvest = False
	transform = False
	solrURL = None
	configurationPath = ''
	try:
		options, arguments = getopt.getopt(sys.argv[1:], 'c:d:hts:', ['config', 'datadir', 'harvest', 'transform', 'solr'])
		for option, value in options:
			if option in ('-c', '--config'):
				configurationPath = value
			elif option in ('-d', '--datadir'):
				dataPath = value
			elif option in ('-h', '--harvest'):
				harvest = True
			elif option in ('-t', '--transform'):
				transform = True
			elif option in ('-s', '--solr'):
				solrURL = value
			else:
				assert False, 'unhandled option'

		repositories = readConfiguration(configurationPath)
		if harvest:
			updateOAI(repositories, configurationPath)

		if transform:
			transformXML(repositories)
			
		if solrURL != None:
			updateSolr(repositories, solrURL)


	except getopt.GetoptError, err:
		usage()
		sys.exit(2)


def readConfiguration(configurationPath):
	# read and parse XML file
	f = open(configurationPath)
	configurationText = f.read()
	f.close()
	XML = xml.etree.ElementTree.fromstring(configurationText)
	
	# loop through <repository> tags to get repositories
	repositories = {}
	XMLRepositories = XML.getiterator('repository')
	for XMLRepository in XMLRepositories:
		attributes = XMLRepository.attrib
		if attributes.has_key('id') and attributes['id'] != '':
			key = attributes['id']
			repositoryInfo = dict()

			baseURLElement = XMLRepository.find('baseUrl')
			if baseURLElement != None and baseURLElement.text != None:
				repositoryInfo['baseURL'] = baseURLElement.text
				nameElement = XMLRepository.find('fullName')
				if nameElement != None:
					repositoryInfo['name'] = nameElement.text

				elementSetElement = XMLRepository.find('set')
				if elementSetElement != None:
					repositoryInfo['elementSet'] = elementSetElement.text
					
				if repositories.has_key(key):
					print u'Repository ID »' + key + u'« used multiple times. Just using the first occurrence.'
				else:
					repositories[key] = repositoryInfo
			else:
				print "Repository »" + key + "« lacks base URL information"
		else:
			print "Repository with blank id in configuration: " + xml.etree.ElementTree.tostring(XMLRepository, encoding='utf-8')
	
	
	
	return repositories



def updateOAI(repositories, configurationPath):
	print ''
	print u'Updating OAI data'
	
	for repositoryID in sorted(repositories.iterkeys()):
		repository = repositories[repositoryID]
		print ''
		print u'Processing repository ID »' + repositoryID + u'«'
		
		repositoryOAIPath = repositoryPath(repositoryID, 'oai')
		
		# Find the latest download and extract its responseDate.
		lastResponseDate = None
		fileList = os.listdir(repositoryOAIPath).sort()
		if fileList != None and len(fileList) > 0:
			newestFile = fileList[-1]
			f = open(newestFile)
			newestFileText = f.read()
			f.close()
			XML = xml.etree.ElementTree.fromstring(newestFileText)
			responseDateElement = XML.find('responseDate')
			if responseDateElement != None:
				lastResponseDate = responseDateElement.text
				print "Last response date: " + lastResponseDate

		# Use the »OaiList.pl« script to download new records from OAI.
		arguments = [downloadScriptPath, '-v', '-c', configurationPath, '-i', repositoryID, '-d', repositoryOAIPath]
		if lastResponseDate != None:
			arguments += ['-f', lastResponseDate]
		print 'Running command: ' + ' '.join(arguments)
		result = subprocess.call(arguments)



def transformXML (repositories):
	print ''
	print u'Applying XSLT to downloaded data'
	
	# Read transformation XSL
	try:
		xslXML = etree.parse('OAI-to-Solr.xsl')
		xsl = etree.XSLT(xslXML)
	except:
		sys.stderr.write('ERROR: Failed to read XSL, stopping transformation')
		return
		
	for repositoryID in sorted(repositories.iterkeys()):
		repository = repositories[repositoryID]
		print ''
		print u'Processing repository ID »' + repositoryID + u'«'
					
		# Run through XML files in oai folder
		repositoryOAIPath = repositoryPath(repositoryID, 'oai')
		if os.path.exists(repositoryOAIPath):
			fileList = os.listdir(repositoryOAIPath)
			if fileList != None:
				if 'token.txt' in fileList:
					fileList.remove('token.txt')
				fileList.sort()
				repositorySolrPath = repositoryPath(repositoryID, 'solr')
				for fileName in fileList:
					solrFilePath = repositorySolrPath + '/' + fileName
					if not os.path.exists(solrFilePath):
						try:
							filePath = repositoryOAIPath + '/' + fileName
							fileXML = etree.parse(filePath)
							solrXML = xsl(fileXML)
							solrFile = open(solrFilePath, 'w')
							solrFile.write(etree.tostring(solrXML, encoding='utf-8', method='xml'))
							solrFile.close()
							print "Converted file »" + fileName + "« to Solr format"
						except:
							print "FAILed to convert file »" + fileName + "«"


def updateSolr (repositories, solrURL):
	print ''
	print u'Adding data to Solr'

	for repositoryID in sorted(repositories.iterkeys()):
		repository = repositories[repositoryID]
		print ''
		print u'Processing repository ID »' + repositoryID + u'«'
		
		updateURL = solrURL + '/update'
		
		repositorySolrPath = repositoryPath(repositoryID, 'solr')
		fileList = os.listdir(repositorySolrPath)
		if fileList != None:
			if 'token.txt' in fileList:
				fileList.remove('token.txt')
			fileList.sort()
			for fileName in fileList:
				solrFilePath = repositorySolrPath + '/' + fileName
				solrFile = open(solrFilePath)
				solrXML = solrFile.read()
				solrFile.close()
				solrRequest = urllib2.Request(updateURL, solrXML)
				solrRequest.add_header('Content-Type', 'text/xml')
				solrConnection = urllib2.urlopen(solrRequest)
				response = solrConnection.read()
				responseCode = solrConnection.info()
				solrConnection.close()
				print u'Uploading »' + fileName + u'« to Solr … '
				
	print u'Commiting Solr Index …'
	solrCommit = urllib2.urlopen(updateURL, "commit=true")
	solrCommit.close()

	


def repositoryPath (repositoryID, dataType):
	path = dataPath + '/' + dataType + '/' + repositoryID
	if not os.path.exists(path):
		os.makedirs(path)
		print "Created folder " + path
	return path

def usage ():
	print 'usage'


if __name__ == "__main__":
    main()
