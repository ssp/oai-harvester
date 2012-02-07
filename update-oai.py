#!/usr/bin/env python
#coding=utf-8
import sys
import os
import getopt
from StringIO import StringIO
import json
import xml.etree
from xml.etree import ElementTree
from lxml import etree
import subprocess
import socket
import urllib2
import shutil

dataPath = 'data'
downloadScriptPath = 'harvester/OaiList.pl'

def main():
	configurationPath = None
	formats = False
	delete = None
	harvest = False
	transform = False
	solrXSL = None
	solrURL = None
	try:
		# evaluate command line parameters
		options, arguments = getopt.getopt(sys.argv[1:], 'c:d:fD:hts:', ['config=', 'datadir=', 'formats', 'delete=', 'harvest=', 'transform', 'solr='])
		for option, value in options:
			if option in ('-c', '--config'):
				configurationPath = value
			elif option in ('-d', '--datadir'):
				dataPath = value
			elif option in ('-D', '--delete'):
				delete = value
			elif option in ('-f', '--formats'):
				formats = True
			elif option in ('-h', '--harvest'):
				harvest = True
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

		# Read transformation XSL if needed
		if transform != None:
			try:
				solrXSLXML = etree.parse('OAI-to-Solr.xsl')
				solrXSL = etree.XSLT(solrXSLXML)
			except:
				printerror('Failed to read XSL for Solr transformation.')

		# loop through repositories and run actions determined by the command line parameters
		for repositoryID in sorted(repositories.iterkeys()):
			repository = repositories[repositoryID]
			repository['ID'] = repositoryID
			
			if not repository.has_key('broken'):
				print ''
				print u'==== ' + repositoryID + u' ===='
				
				if formats:
					determineFormats(repository)
				
				if harvest:
					updateOAI(repository, configurationPath)

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
				printerror('Failed to commit the Solr index: ' + str(err))
		
	except getopt.GetoptError, err:
		printerror('Could not parse the options: ' + str(err))
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
	path = repositoryPath(None, dataType)
	print "Deleting folder »" + path + "« …"
	shutil.rmtree(path)



def determineFormats (repository):
	print u'Supported formats:'
		
	timeout = 10
	URL = repository['url'] + '?verb=ListMetadataFormats'
	try:
		formatsConnection = urllib2.urlopen(URL, None, timeout)
		formatsString = formatsConnection.read()
		formatsConnection.close()
		formatsXML = xml.etree.ElementTree.fromstring(formatsString)
		formats = formatsXML.iter('{http://www.openarchives.org/OAI/2.0/}metadataFormat')
		for format in formats:
			output = repository['ID'] + '\t'
			prefix = ''
			prefixElement = format.find('{http://www.openarchives.org/OAI/2.0/}metadataPrefix')
			if prefixElement != None and prefixElement != None:
				prefix = prefixElement.text
			else:
				printerror('Metadata format without prefix: ' + str(prefixElement))

			namespace = ''
			namespaceElement = format.find('{http://www.openarchives.org/OAI/2.0/}metadataNamespace')
			if namespaceElement != None and namespaceElement.text != None:
				namespace = namespaceElement.text

			print repository['ID'] + '\t' + prefix + '\t' + namespace
			
	except urllib2.URLError as err:
		printerror('Could not retrieve metadata formats: ' + str(err))
	except xml.etree.ElementTree.ParseError as err:
		printerror('Could not parse XML of presumed metadata information: ' + str(err))
	except socket.timeout as err:
		printerror('Gave up after ' + str(timeout) + ' second timeout.')



def updateOAI (repository, configurationPath):
	print u'Harvest OAI data'
	repositoryOAIPath = repositoryPath(repository, 'oai')	
	# Find the latest commited download and extract its responseDate.
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

	repositoryOAITempPath = repositoryPath(repository, 'oai-temp', True)
	# Use the »OaiList.pl« script to download new records from OAI.
	arguments = [downloadScriptPath, '-v', '-c', configurationPath, '-i', repository['ID'], '-d', repositoryOAITempPath]
	if lastResponseDate != None:
		arguments += ['-f', lastResponseDate]
	print 'Running command: ' + ' '.join(arguments)
	result = subprocess.call(arguments)



def transformXML (repository):
	# Run through XML files in oai-temp folder
	OAITempPath = repositoryPath(repository, 'oai-temp')
	OAIPath = repositoryPath(repository, 'oai')
	solrTempPath = repositoryPath(repository, 'solr-temp', True)
	if os.path.exists(OAITempPath):
		fileList = os.listdir(OAITempPath)
		if fileList != None:
			if 'token.txt' in fileList:
				fileList.remove('token.txt')
			fileList.sort()
			for fileName in fileList:
				print fileName
				solrFilePath = solrTempPath + '/' + fileName
				OAIFilePath = OAITempPath + '/' + fileName
				try:
					fileXML = etree.parse(OAIFilePath)
					solrXML = xsl(fileXML, collections="'geoleo-oai'")
					solrFile = open(solrFilePath, 'w')
					solrFile.write(etree.tostring(solrXML, encoding='utf-8', method='xml'))
					solrFile.close()
					print "Created Solr file »" + solrFilePath + "«"
					moveFile(fileName, OAITempPath, OAIPath)
					
				except:
					printerror("Could not convert file »" + fileName + "«", repository)



def updateSolr (repository, solrURL):
	updateURL = solrURL + '/update'
	
	repositorySolrTempPath = repositoryPath(repository, 'solr-temp')
	repositorySolrPath = repositoryPath(repository, 'solr')
	fileList = os.listdir(repositorySolrTempPath)
	print repositorySolrTempPath
	if fileList != None:
		if 'token.txt' in fileList:
			fileList.remove('token.txt')
		fileList.sort()
		for fileName in fileList:
			solrTempFilePath = repositorySolrTempPath + '/' + fileName
			solrFile = open(solrTempFilePath)
			solrXML = solrFile.read()
			solrFile.close()
			solrRequest = urllib2.Request(updateURL, solrXML)
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
	print "Moving »" + oldPath + "« to »" + newPath + "«"
	os.rename(oldPath, newPath)



def repositoryPath (repository, dataType, emptyFolder = False):
	path = dataPath
	
	if dataType != None:
		path = path + '/' + dataType
		if repository != None:	
			path = path + '/' + repository['ID']
			
	if not os.path.exists(path):
		os.makedirs(path)
		print "Created folder »" + path + "«"
	elif emptyFolder == True:
		if os.listdir(path) != None:
			if len(os.listdir(path)) > 0:
				print "Removing " + str(len(os.listdir(path))) + " files from »" + path + "«"
				shutil.rmtree(path)
				os.makedirs(path)

	return path



def printerror (message, repository = None):
	output = 'ERROR: '
	if repository != None:
		output = output + '(' + repository['ID'] + ') '
	output = output + message 
	print >> sys.stderr, output
	
	

if __name__ == "__main__":
    main()
