#!/usr/local/bin/perl

# © 2007, The Regents of The University of Michigan, All Rights Reserved
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject
# to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

use strict;
use Getopt::Std;
use XML::LibXML;
use POSIX qw(strftime);
use DBI qw(:sql_types);


## config vars -- must change to use
my $dbUser   = "user";
my $dbPasswd = "passwd";
my $dbName   = "foo";
my $dbServer = "bar";
## config vars -- must change to use

## optional config -- xpath to find records
my $recordXpath = "/records/record";

my %opts;
getopts('d:s:vh', \%opts);

my $dataDir = $opts{'d'};
my $oaiSet  = $opts{'s'};
my $help    = $opts{'h'};
my $verbose = $opts{'v'};

if ( $help || ! $dataDir ) 
{ 
    die "USAGE: $0 -d data_dir (location of XML data) " .
        "\n\t[-s oai_set] " .
        "\n\t[-h (this help message)] " .
        "\n\t[-v (verbose)]\n"; 
}

my $parser = new XML::LibXML;
my $dbh    = ConnectToDb( $dbUser, $dbPasswd, $dbName, $dbServer );

## ============================================================================ ##
## load from Records files
## 
opendir my $dirFH, $dataDir or die "failed to open: $dataDir: $@ \n";
my @dirs = grep { /\.xml$/ } readdir( $dirFH );
closedir $dirFH;

foreach my $xmlFile ( @dirs )
{
    open ( my $xmlFileFH, "$dataDir/$xmlFile" ) 
        or die "failed to open $dataDir/$xmlFile: $@ \n";
    my $fileName = "$dataDir/$xmlFile";

    if ( ! -f $fileName ) { print "no file found $fileName \n"; next; }

    my $source = $parser->parse_file( $fileName );

    foreach my $recordNode ( $source->findnodes( $recordXpath ) )
    {
        my ($idNode)   = $recordNode->findnodes( "./*[name()='header']/*[name()='identifier']" );
        my $identifier = $idNode->textContent();
        my $format     = GetFormat( $recordNode );

        if (! $oaiSet) { $oaiSet = $recordNode->findvalue( "./*[name()='header']/*[name()='setSpec']" ); }

        my @parts = split( ":", $identifier);
        my $ident = $parts[-1];

        my ($metadata)  = $recordNode->findnodes( "./*[name()='metadata']" );
        my $metadataStr = $metadata->toString();

        if ($verbose) { print "store: $ident, $oaiSet $format \n"; }

        StoreRecord($ident, $oaiSet, $format, $metadataStr); 
    }
    $dbh->commit();
}


## ============================================================================ ##
## ============================================================================ ##

## ----------------------------------------------------------------------------
##  Function:   store a record (insert or update)
##  Parameters: ID, date, oai set, formar (oai_dc || marc21), data
##  Return:     nothing -- puts data in DB
## ----------------------------------------------------------------------------
sub StoreRecord
{
    my $id     = shift;
    my $set    = shift;
    my $format = shift;
    my $data   = shift;

    my $select   = qq{ SELECT id FROM oai WHERE id = \"$id\" };
    my $response = $dbh->selectall_arrayref( $select );

    my $sth;
    if ( $format eq "delete" )
    {
        my $sqlDel = qq{ UPDATE oai SET oai_dc = NULL WHERE id = \"$id\" };
        $sth       = $dbh->prepare( $sqlDel );
    }
    elsif ( scalar(@$response) == 1 )
    {
        if ($verbose) { print "update $id \n"; }
        my $sqlUpdate = qq{ UPDATE oai set $format = ? WHERE id = \"$id\" };
        $sth          = $dbh->prepare( $sqlUpdate );
        $sth->bind_param(1, $data, SQL_BLOB);
    }
    elsif ( scalar(@$response) > 1 )
    {
        print "multiple rows for $id: \n";
        return;
    }
    else
    {
        my $sqlInsert = qq{ INSERT INTO oai (id,$format) VALUES (\"$id\", ?) };
        $sth          = $dbh->prepare( $sqlInsert );
        $sth->bind_param(1, $data, SQL_BLOB);
    }

    eval { $sth->execute(); };
    if ($@)
    {
        print "STORE failed for $id, rollback :$@ \n";
        eval { $dbh->rollback() };
        die "Couldn't roll back\n" if $@;
    }

    ## now update oaisets table
    if ( $format ne "delete" )
    {
        my $setSth = $dbh->prepare( qq{ REPLACE INTO oaisets (id, oaiset) VALUES (\"$id\", \"$set\") } );
        eval { $setSth->execute(); };
        if ($@)
        {
            print "STORE failed for $id, rollback :$@ \n";
            eval { $dbh->rollback() };
            die "Couldn't roll back\n" if $@;
        }
    }
}

## ----------------------------------------------------------------------------
##  Function:   determine the format in the record node
##  Parameters: ref to libxml record node
##  Return:     STRING (format: oai_dc, marc21...)
## ----------------------------------------------------------------------------
sub GetFormat
{
    my $node = shift;

    my ($metadata) = $node->findnodes( "./*[name()='metadata']" );

    ## dc => oai_dc, record => marc21, what else?  Is that right?
    if    ( $metadata->findnodes( "./*[local-name()='dc']" ) )     { return "oai_dc"; }
    elsif ( $metadata->findnodes( "./*[local-name()='record']" ) ) { return "marc21"; }
    elsif ( $metadata->findnodes( "./*[local-name()='mods']"   ) ) { return "mods";   }

    return "";
}

## ----------------------------------------------------------------------------
##  Function:   connect to the mysql DB
##  Parameters: nothing
##  Return:     ref to DBI
## ----------------------------------------------------------------------------
sub ConnectToDb 
{
    my $db_user   = shift;
    my $db_passwd = shift;
    my $db_name   = shift;
    my $db_server = shift;

    my $dbh = DBI->connect( "DBI:mysql:$db_name:$db_server", $db_user, $db_passwd,
              { RaiseError => 1, AutoCommit => 0 } ) || die "Cannot connect: $DBI::errstr";

    my $res = $dbh->selectall_arrayref( "SHOW TABLES LIKE \"oai\"" );
    if ( ! scalar @$res) { die "no oai table \n"; }
    return $dbh;
}

__END__


=head1 NAME

loadOai -- Generic script for loading oai data into database 

=head1 SYNOPSIS

This script looks for XML files in the data dir and loads each record into the oai DB table

=head1 DESCRIPTION

Before using you must change the DB connection settings at the top of this script:

    my $dbUser   = "user";
    my $dbPasswd = "passwd";
    my $dbName   = "foo";
    my $dbServer = "bar";

The available arguments for the script are:
   -d data dir (place to find the XML data)
   -s oai set (optional)
   -h (help: message printed)
   -v (generates verbose output which is stored in the loading Log)

The only required arguments are the data dir (-d).

All of the data within and including the <metadata> element is loaded into the DB under the specified format.  This data is not validated or checked in any way other than to make sure it is well formed XML.  

If the oai set (-s) is used, the setSpec is not checked in the /record/header/ element.

For the identifier, the script only cares about everything after the last ":" (oai:host:id).

=head1 DB format

Example of create table statement: 

CREATE TABLE oai (id VARCHAR(20) PRIMARY KEY, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TI
MESTAMP, oaiset VARCHAR(10), oai_dc MEDIUMBLOB, marc21 MEDIUMBLOB);

=head1 XML FORMAT

The XML files must have the OAI record elements wrapped in a <records> element:

By default, this script will look for record elements wrapped in a <records> tag.  If you have the data nested some other way, just change $recordXpath to the correct xpath to find the record elements.  Here's an example XML file:

  <?xml version="1.0" encoding="UTF-8"?>
  <records>
    <record>
      <header>
        <identifier>oai:some.host..edu:id-1234</identifier>
        <datestamp>2007-10-22T15:43:11Z</datestamp>
        <setSpec>foo</setSpec>
      </header>
      <metadata> [ ... ]
      </metadata>
    </record>
    <record> [ ... ] </record>
  </records>

There can be multiple record elements in a single file as well as multiple ".xml" files in the data directory.

