#!/usr/bin/env python
#coding=utf-8
import sys
import os
import getopt
import xml.etree
from xml.etree import ElementTree
import subprocess

dataPath = 'data'
downloadScriptPath = 'harvester/OaiList.pl'

def main():
	configurationPath = ''
	try:
		options, arguments = getopt.getopt(sys.argv[1:], 'c:d:', ['config', 'datadir'])
		for option, value in options:
			if option in ('-c', '--config'):
				configurationPath = value
			elif option in ('-d', '--datadir'):
				dataPath = value
			else:
				assert False, 'unhandled option'

		repositories = readConfiguration(configurationPath)
		updateOAI(repositories, configurationPath)

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
	for repositoryID in repositories:
		repository = repositories[repositoryID]
		
		# Create folder for repository if necessary.
		repositoryOAIPath = dataPath + '/oai/' + repositoryID
		if not os.path.exists(repositoryOAIPath):
			os.makedirs(repositoryOAIPath)
			print "Created folder " + repositoryOAIPath
		
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

		# Use the »OaiList.pl« script to download new records from OAI.
		arguments = [downloadScriptPath, '-v', '-c', configurationPath, '-i', repositoryID, '-d', repositoryOAIPath]
		if lastResponseDate != None:
			arguments += ['-f', lastResponseDate]
		print arguments
		result = subprocess.call(arguments)



def usage():
	print 'usage'


if __name__ == "__main__":
    main()
