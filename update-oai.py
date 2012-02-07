#!/usr/bin/env python
#coding=utf-8
import sys
import os
import getopt
import json
import xml.etree
from xml.etree import ElementTree
from lxml import etree
import subprocess
import socket
import urllib
import urllib2
import httplib
import shutil

dataPath = 'data'
OAI2Namespace = '{http://www.openarchives.org/OAI/2.0/}'
maximumChunks = 10000

def main():
	configurationPath = u'config/config.js'
	formats = False
	delete = None
	harvest = None
	transform = False
	solrXSL = None
	solrURL = None
	try:
		# evaluate command line parameters
		options, arguments = getopt.getopt(sys.argv[1:], 'c:d:fD:h:ts:', ['config=', 'datapath=', 'formats', 'delete=', 'harvest=', 'transform', 'solr='])
		for option, value in options:
			if option in ('-c', '--config'):
				configurationPath = value
			elif option in ('-d', '--datapath'):
				dataPath = value
			elif option in ('-D', '--delete'):
				delete = value
			elif option in ('-f', '--formats'):
				formats = True
			elif option in ('-h', '--harvest'):
				harvest = value
			elif option in ('-t', '--transform'):
				transform = True
			elif option in ('-s', '--solr'):
				solrURL = value
				print value
			else:
				assert False, 'unhandled option'

		# read configuration
		f = open(configurationPath)
		configuration = json.load(f)
		f.close()
		repositories = configuration['servers']
		
		if delete != None:
			deleteFiles(delete)

		# Read transformation XSL if needed.
		if transform != None:
			try:
				solrXSLXML = etree.parse('OAI-to-Solr.xsl')
				solrXSL = etree.XSLT(solrXSLXML)
			except:
				printError('Failed to read XSL for Solr transformation.')

		# loop through repositories and run actions determined by the command line parameters
		for repositoryID in sorted(repositories.iterkeys()):
			repository = repositories[repositoryID]
			repository['ID'] = repositoryID
			
			if not repository.has_key('broken'):
				print ''
				printHeading(u'==== ' + repositoryID + u' ====', repository)
				
				if formats:
					determineFormats(repository)
				
				if harvest != None:
					updateOAI(repository, configuration, harvest.split(','))

				if transform:
					transformXML(repository, solrXSL)
			
				if solrURL != None:
					updateSolr(repository, solrURL)
	
		if solrURL != None:
			print ''
			print u'Committing Solr Index …'
			try:
				solrCommit = urllib2.urlopen(updateURL, "commit=true")
				solrCommit.close()
			except urllib2.URLError as err:
				printError('Failed to commit the Solr index: ' + str(err))
		
	except getopt.GetoptError, err:
		printError('Could not parse the options: ' + str(err))
		sys.exit(2)



def deleteFiles (delete):
	if delete in ('oai', 'temp', 'all'):
		deleteData('oai-temp')
	if delete in ('oai', 'all'):
		deleteData('oai')
	if delete in  ('solr', 'temp', 'all'):
		deleteData('solr-temp')
	if delete in ('solr', 'all'):
		deleteData('solr')



def deleteData (dataType):
	path = repositoryPath(dataType)
	print u'Deleting folder »' + path + u'«'
	shutil.rmtree(path)



def determineFormats (repository):
	printHeading (u'Supported formats:')
		
	formatsXML = runOAIRequest(repository, 'ListMetadataFormats', timeout = 10)
	if formatsXML != None:
		formats = formatsXML.iter(OAI2Namespace + 'metadataFormat')
		for format in formats:
			output = repository['ID'] + '\t'
			prefix = ''
			prefixElement = format.find(OAI2Namespace + 'metadataPrefix')
			if prefixElement != None and prefixElement != None:
				prefix = prefixElement.text
			else:
				printError('Metadata format without prefix: ' + str(prefixElement))

			namespace = ''
			namespaceElement = format.find(OAI2Namespace + 'metadataNamespace')
			if namespaceElement != None and namespaceElement.text != None:
				namespace = namespaceElement.text

			print repository['ID'] + '\t' + prefix + '\t' + namespace
	else:
		printError('Could not list metadata formats.', repository)



def updateOAI (repository, configuration, collections):
	# Use existing sets or create a single placeholder one if we don’t need them.
	sets = {None: None}
	if repository.has_key('sets'):
		sets = repository['sets']
	# Loop over sets.
	for setID in sets:
		set = sets[setID]

		# Do we need to harvest this set?
		needToHarvest = (collections == ['all'])
		if not needToHarvest:
			folderName = repository['ID']
			if setID != None:
				folderName = folderName + u'_' + setID
			for collection in collections:
				if folderName in configuration['collections'][collection]:
					needToHarvest = True
					break
							
		if needToHarvest:
			heading = u'Harvest OAI data'
			if setID != None:
				heading = heading + ' for set ' + setID + ' (' + set + ')'
			printHeading(heading)

			# Find the latest commited download and extract its responseDate.
			lastResponseDate = None
			repositoryOAIPath = repositoryPath('oai', repository, setID)
			fileList = os.listdir(repositoryOAIPath)
			fileList.sort()
			if fileList != None and len(fileList) > 0:
				newestFile = repositoryOAIPath + '/' + fileList[-1]
				print newestFile 
				f = open(newestFile)
				newestFileText = f.read()
				f.close()
				XML = xml.etree.ElementTree.fromstring(newestFileText)
				responseDateElement = XML.find(OAI2Namespace + 'responseDate')
				if responseDateElement != None and responseDateElement.text != None:
					lastResponseDate = responseDateElement.text.split('T')[0]
					print u'Last response date: ' + lastResponseDate
					
			# Configure and run initial OAI request.
			options = {'metadataPrefix': configuration['format']}
			if repository.has_key('format'):
				options['metadataPrefix'] = repository['format']
			if lastResponseDate != None:
				options['from'] = lastResponseDate
			if setID != None:
				options['set'] = set
			recordsXML = runOAIRequest(repository, 'ListRecords', options)
			
			# Loop until no more data comes from the server.
			repositoryOAITempPath = repositoryPath('oai-temp', repository, setID, True)
			fileCount = 0
			while recordsXML != None:
				# Store XML to temp folder.
				fileCount = fileCount + 1
				XMLString = xml.etree.ElementTree.tostring(recordsXML, encoding='utf-8')
				path = repositoryOAITempPath + '/' + '%09d'%(fileCount) + '.xml'
				XMLFile = open(path, 'w')
				XMLFile.write(XMLString)
				XMLFile.close()
				print u'Wrote file »' + path + u'«'
			
				listRecordsElement = recordsXML.find(OAI2Namespace + 'ListRecords')
				if listRecordsElement != None:
					tokenElement = recordsXML.find(OAI2Namespace + 'ListRecords' + '/' + OAI2Namespace + 'resumptionToken')
					# Extract resumption token.
					if tokenElement != None and tokenElement.text != None:
						options = {'resumptionToken': tokenElement.text}
						recordsXML = runOAIRequest(repository, 'ListRecords', options)
					else:
						break
						
					# See whether there are any records in the download.
					# If there aren’t, stop downloading (to deal with repositories
					# which violate the specification and return a non-empty resumption token
					# when there are no results)
					recordElements = listRecordsElement.findall(OAI2Namespace + 'record')
					if len(recordElements) == 0:
						printError('Empty list of records: stopping', repository)
						break
					
				else:
					printError('No »ListRecords« element in the download: stopping', repository)
					
				if fileCount > maximumChunks:
					printError(u'Downloaded more than ' + maximumChunks + u': stopping.', repository)
				
			else:
				printError('Could not list records.', repository)
	


def runOAIRequest (repository, verb, parameters = {}, timeout = 30):
	parameters['verb'] = verb
	URL = repository['url'] + '?' + urllib.urlencode(parameters)
	XMLString = getURL(URL, timeout)
	XML = None
	try:
		XML = xml.etree.ElementTree.fromstring(XMLString)
		
		# Check for error.
		errorElement = XML.find(OAI2Namespace + 'error')
		if errorElement != None:
			errorMessage = 'OAI Error: '
			if errorElement.attrib.has_key('code'):
				errorMessage = errorElement.attrib['code'] + u' ' + errorMessage
			if errorElement.text != None:
				errorMessage = errorMessage + errorElement.text
			printError(errorMessage, repository)
			XML = None
		
	except xml.etree.ElementTree.ParseError as err:
		printError(u'Could not parse XML: ' + str(err), repository)
	
	return XML
	
	


def getURL (URL, timeout = 30):
	result = None
	try:
		print u'Loading »' + URL + u'«'
		connection = urllib2.urlopen(URL, None, timeout)
		result = connection.read()
		connection.close()
	
	except httplib.InvalidURL as err:
		printError(u'Invalid URL »' + URL + u'«: ' + str(err))
	except urllib2.URLError as err:
		printError(u'Could not load URL ' + str(err))
	except socket.timeout as err:
		printError(u'Gave up after ' + str(timeout) + u' second timeout.')
	
	return result


	

def transformXML (repository, XSL):
	printHeading('Transforming OAI records to Solr records')
	# Run through XML files in oai-temp folder
	OAITempPath = repositoryPath('oai-temp', repository)
	OAIPath = repositoryPath('oai', repository)
	solrTempPath = repositoryPath('solr-temp', repository, None, True)
	if os.path.exists(OAITempPath):
		fileList = os.listdir(OAITempPath)
		if fileList != None:
			fileList.sort()
			for fileName in fileList:
				solrFilePath = solrTempPath + '/' + fileName
				OAIFilePath = OAITempPath + '/' + fileName
				try:
					fileXML = etree.parse(OAIFilePath)
					solrXML = XSL(fileXML, collections="'geoleo-oai'")
					solrFile = open(solrFilePath, 'w')
					solrFile.write(etree.tostring(solrXML, encoding='utf-8', method='xml'))
					solrFile.close()
					print u'Created Solr file »' + solrFilePath + u'«'
					moveFile(fileName, OAITempPath, OAIPath)
					
				except:
					printError(u'Could not convert file »' + fileName + u'«', repository)



def updateSolr (repository, solrURL):
	printHeading('Uploading to Solr')
	
	repositorySolrTempPath = repositoryPath('solr-temp', repository)
	repositorySolrPath = repositoryPath('solr', repository)
	fileList = os.listdir(repositorySolrTempPath)
	if fileList != None:
		fileList.sort()
		for fileName in fileList:
			solrTempFilePath = repositorySolrTempPath + '/' + fileName
			solrFile = open(solrTempFilePath)
			solrXML = solrFile.read()
			solrFile.close()
			solrRequest = urllib2.Request(solrURL + '/update', solrXML)
			solrRequest.add_header('Content-Type', 'text/xml')
			solrConnection = urllib2.urlopen(solrRequest)
			response = solrConnection.read()
			responseCode = solrConnection.info()
			solrConnection.close()
			print u'Uploading »' + fileName + u'« to Solr … '
			moveFile(fileName, repositorySolrTempPath, repositorySolrPath)



def moveFile (name, oldFolder, newFolder):
	oldPath = oldFolder + '/' + name
	fileList = os.listdir(newFolder)
	fileCount = 0
	if fileList != None:
		fileCount = len(fileList)
	newPath = newFolder + '/' + '%09d'%(fileCount + 1) + '.xml'
	print u'Moving »' + oldPath + u'« to »' + newPath + u'«'
	os.rename(oldPath, newPath)



def repositoryPath (dataType, repository = None, dataSet = None, emptyFolder = False):
	path = dataPath
	
	if dataType != None:
		path = path + '/' + dataType
		if repository != None:
			path = path + '/' + repository['ID']
			if dataSet != None:
				path = path + '_' + dataSet
			
		if not os.path.exists(path):
			os.makedirs(path)
			print u'Created folder »' + path + u'«'
		elif emptyFolder == True:
			if os.listdir(path) != None:
				if len(os.listdir(path)) > 0:
					print u'Removing ' + str(len(os.listdir(path))) + u' file(s) from »' + path + u'«'
					shutil.rmtree(path)
					os.makedirs(path)
	else:
		printError(u'Need a dataType to determine the repositoryPath.', repository)

	return path



def printHeading (message, repository = None):
	print bcolors.HEADER + message + bcolors.ENDC



def printError (message, repository = None):
	output = bcolors.FAIL + u'ERROR: ' + bcolors.ENDC
	if repository != None:
		output = output + u'(' + repository['ID'] + u') '
	output = output + message 
	print >> sys.stderr, output



"""
Colour for terminal output.
Found at: http://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
"""
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''


if __name__ == "__main__":
    main()
