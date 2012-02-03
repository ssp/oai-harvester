<?xml version="1.0" encoding="UTF-8"?>
<!--
	Sven-S. Porst, SUB Göttingen <porst@sub.uni-goettingen.de>
-->
<xsl:stylesheet
	version="1.0"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:dc="http://purl.org/dc/elements/1.1/"
	xmlns:oai="http://www.openarchives.org/OAI/2.0/"
	xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">

	<xsl:output indent="yes" method="xml" version="1.0" encoding="UTF-8"/>

	<xsl:template match="oai:OAI-PMH">
		<xsl:apply-templates select="*"/>
	</xsl:template>

	<xsl:template match="oai:responseDate">
	</xsl:template>
	
	<xsl:template match="oai:request">
	</xsl:template>

	<xsl:template match="oai:ListRecords">
		<add>
			<xsl:for-each select="oai:record[not(oai:header/@status) or oai:header/@status!='deleted']">
				<xsl:apply-templates select="."/>
			</xsl:for-each>
		</add>
		<delete>
			<xsl:for-each select="oai:record[oai:header/@status='deleted']">
				<xsl:apply-templates select="."/>
			</xsl:for-each>
		</delete>
	</xsl:template>

	<xsl:template match="oai:record[not(oai:header/@status) or oai:header/@status!='deleted']">
		<doc>
			<xsl:for-each select="*">
				<xsl:apply-templates select="."/>
			</xsl:for-each>
		</doc>
	</xsl:template>

	<xsl:template match="oai:record[oai:header/@status='deleted']">
		<id>
			<xsl:value-of select="oai:header/oai:identifier"/>
		</id>
	</xsl:template>


	<xsl:template match="oai:header">
		<xsl:apply-templates select="*"/>
	</xsl:template>

	<xsl:template match="oai:setSpec">
	</xsl:template>

	<xsl:template match="oai:identifier">
		<field name="id">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="oai:datestamp">
		<field name="last_modified">
			<xsl:value-of select="."/>
			<xsl:if test="string-length(.) &lt;= 10">
				<xsl:text>T23:59:59Z</xsl:text>
			</xsl:if>
		</field>
	</xsl:template>



	<xsl:template match="oai:metadata">
		<xsl:apply-templates select="*"/>
	</xsl:template>

	<xsl:template match="oai_dc:dc">
		<xsl:apply-templates select="*"/>
	</xsl:template>

	<xsl:template match="dc:title">
		<field name="title">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:creator">
		<field name="author">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:contributor">
		<field name="contributor">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:publisher">
		<field name="publisher">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:subject">
		<field name="subject">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:description">
		<field name="description">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:date">
		<field name="date">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:type">
		<field name="type">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:language">
		<field name="language">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:format">
		<field name="content_type">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:identifier">
		<field name="identifier">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:relation">
		<field name="links">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:rights">
		<field name="rights">
			<xsl:value-of select="."/>
		</field>
	</xsl:template>

	<xsl:template match="dc:source">
<!--
		<field name="source">
			<xsl:value-of select="."/>
		</field>
-->
	</xsl:template>

	<xsl:template match="dc:coverage">
<!--
		<field name="coverage">
			<xsl:value-of select="."/>
		</field>
-->
	</xsl:template>


	<xsl:template match="*">
		<field name="ignoredField">
			<xsl:value-of select="name(.)"/>
		</field>
	</xsl:template>


	<xsl:template match="text()"/>
</xsl:stylesheet>
