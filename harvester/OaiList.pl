#!/usr/bin/env perl

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

$! =1;

use strict;
use Getopt::Std;
use LWP::UserAgent;
use XML::LibXML;


my $dataDir              = "oai/";

## ============================================================================ ##
## ============================================================================ ##
##  
##  
## ============================================================================ ##

my $parser = new XML::LibXML;
my %opts;
getopts('c:wt:vri:s:m:f:u:x:d:', \%opts);

my $repositoryConfigFile = $opts{'c'};
my $id           = $opts{'i'};
my $set          = $opts{'s'};
my $mdFormat     = $opts{'m'};
my $from         = $opts{'f'};
my $until        = $opts{'u'};
my $max          = $opts{'x'};
my $resume       = $opts{'r'};
my $dataD        = $opts{'d'};
my $type         = $opts{'t'};
my $verbose      = $opts{'v'};
my $writeRecords = $opts{'w'};

if ( $id eq "" || $repositoryConfigFile eq "" )
{ 
    die "\nUSAGE: $0 -c repository_config_file" .
        "\n\t-i repo_id" .
        "\n\t[-t type_of_list (default: records) || (identifiers)]" .
        "\n\t[-s set_id (other than what is in config)] " .
        "\n\t[-m metadata_format (default oai_dc)]" .
        "\n\t[-f from (YYYY-MM-DDThh:mm:ssZ)]" .
        "\n\t[-u until (YYYY-MM-DDThh:mm:ssZ)]" .
        "\n\t[-x max_request (default 100000)]" .
        "\n\t[-w (write individual record files, not a ListRecords file)]" . 
        "\n\t[-d data_dir]" .
        "\n\t[-v (verbose)]" .
        "\n\t[-r (to resume using token file)\n"; 
}


my %repositories = &GetRepositories ( $id );
my $repoUrl      = $repositories{$id}->{'url'};
my $repoSet      = $repositories{$id}->{'set'};
my $maxRequests  = 100000;
my $maxRetry     = 5;

if ( $max )        { $maxRequests = $max; } 
if ( ! $mdFormat ) { $mdFormat    = "oai_dc"; }
if ( $set )        { $repoSet     = $set; }
if ( $dataD )      { $dataDir     = $dataD; }

my $tokenFile = "$dataDir/token.txt";

my $done = 0;
my ($counter, $resumptionToken) = (GetToken());

if ( $resume ) { $maxRequests += $counter; }

my $verb;
if    ( $type eq "identifiers" ) { $verb = "ListIdentifiers"; }
elsif ( $type eq "records" )     { $verb = "ListRecords"; }
else                             { $verb = "ListRecords"; }

while ( ! $done ) 
{
    my $url;
    if ( $resumptionToken eq "" ) 
    {
        $url = "$repoUrl?verb=$verb&metadataPrefix=$mdFormat";
        if ( $repoSet ne "" ) { $url .= "&set=$repoSet"; }
        if ( $from )  { $url .= "&from=$from"; }
        if ( $until ) { $url .= "&until=$until"; }

        if ($verbose) { print "GET: $url \n"; }
    }
    else
    {
        $url = "$repoUrl?verb=$verb&resumptionToken=$resumptionToken";
        if ($verbose) { print "GET: $url \n"; }
    }

    my $response = GetOAIResponse( $url );
    $maxRetry    = 5; ## reset retry

    my $source;
    eval { $source = $parser->parse_string( $response ); };
    if ($@) { die "failed to parse response:$@ \n"; }

    ($resumptionToken) = $source->findvalue( "//*[name()='resumptionToken']" );
    my $errorCode      = $source->findvalue( "//*[name()='error']" );

    if ( $writeRecords ) { WriteRecordFiles( $source ); }
    else                 { WriteFile( $source ); }

    if ( $errorCode )
    {
        $done = 1;
        if ($verbose) { print "ERROR: $errorCode \n"; }
    }

    if ( $resumptionToken eq "" )     { $done = 1; }
    if ( $counter++ >= $maxRequests ) { $done = 1; }
}

if ( $resumptionToken ne "" ) { WriteToken( $resumptionToken, $counter ); }


## ============================================================================ ##
## subs
##
sub GetRepositories 
{
    my ( $id ) = @_;
    my @ids = split (/,/,$id);

    my $parser = new XML::LibXML;
    my (%repositories, $source);
    eval { $source = $parser->parse_file( $repositoryConfigFile ); };
    die "failed to load $repositoryConfigFile: $@ \n" if $@;

    foreach my $repositoryNode ( $source->findnodes( "/RepositoryConfig/repository" ) )
    {
        my $repoID = $repositoryNode->findvalue( '@id' );

        if ( grep /^$repoID$/, @ids )
        {
            my $baseUrl = $repositoryNode->findvalue( "baseUrl" );
            my $repoSet = $repositoryNode->findvalue( "set" );
            $repositories{$repoID}->{'url'} = $baseUrl;
            $repositories{$repoID}->{'set'} = $repoSet;
        }
    }
    return %repositories;
}

sub GetOAIResponse 
{
    my $url = shift;

    my $ua = LWP::UserAgent->new;
    $ua->timeout( 1000 ); ## # of seconds
    my $req = HTTP::Request->new( GET => $url );
    my $res = $ua->request( $req );

    if ($res->is_success) { return $res->content; }
    else 
    { 
        if ( $maxRetry-- > 0 ) 
        {
            print "retry GET in " . (10 * (5 - $maxRetry ) ) . " \n";
            WriteToken( $resumptionToken, $counter );
            sleep (10 * (5 - $maxRetry ) );
            GetOAIResponse( $url );
        } 
        else
        {
            WriteToken( $resumptionToken, $counter );
            die "ERROR: request $url failed: " .$res->status_line() ." : " . $res->message() ."\n"; 
        }
    }
}

sub GetToken
{
    if ( ! $resume ) { return (1, ""); }
    if ( -f $tokenFile ) 
    {
        open( my $fh, $tokenFile ) || return "";
        my $data = do { local $/; <$fh> };
        my ($token, $c) = split(/\t/, $data);
        chomp $token;
        if ( $token ne "" ) { return ($c, $token); }
    }
    return (1, "");
}

sub WriteToken
{
    my $token = shift;
    my $count = shift;
    open( my $fh, ">", $tokenFile ) || die "failed to write token: $tokenFile: $@ \n";
    print $fh "$token\t$count";
    close $fh;
}

sub WriteRecordFiles
{
    my $source = shift;

    foreach my $recordNode ( $source->findnodes( "//*[local-name()='ListRecords']/*[local-name()='record']" ) )
    {
        my $id    = $recordNode->findvalue( "*[local-name()='header']/*[local-name()='identifier']" );
        my @parts = split(":", $id);
        if ( $parts[-1] eq "" ) { die "failed to get ID \n"; }

        my $recordName = $dataDir ."/record_". $mdFormat ."_". $parts[-1] . ".xml";

        my $openOp = ">";
        ## hack to work around old libxml problem...
        my ($libmaj, $libmin, $librev) = split(/\./, XML::LibXML::LIBXML_DOTTED_VERSION );
        if ( ($libmin <= 6) && ($librev < 26) ) { $openOp .= ":utf8"; }

        if ( open( my $fh, $openOp, $recordName) )
        {
            print $fh qq{<?xml version="1.0" encoding="UTF-8"?>} . "\n";
            print $fh $recordNode->toString(1);
            close $fh;
        }
        else { die "failed to open $recordName \n"; }
    }
}

sub WriteFile
{
    my $source = shift;

    my $padCounter = sprintf("%0*d", "8", $counter);
    my $recordFileName = $dataDir ."/". $verb ."_". $mdFormat ."_". $padCounter .".xml";

    my $openOp = ">";
    ## hack to work around old libxml problem...
    my ($libmaj, $libmin, $librev) = split(/\./, XML::LibXML::LIBXML_DOTTED_VERSION );
    if ( ($libmin <= 6) && ($librev < 26) ) { $openOp .= ":utf8"; }

    if ( open( my $fh, $openOp, $recordFileName) )
    {
        print $fh $source->toString(1);
        close $fh;
    }
    else { die "failed to open $recordFileName \n"; }
}


__END__

=head1 NAME

OaiList -- Script for retrieving oai records from an OAI provider.

=head1 SYNOPSIS

This file harvests the oai records from an OAI provider and stores them as XML files. The xml files are stored in /$DLXSROOT/prep/o/oaip unless the -d option is used. The script loads the repository configuration file from RepositoryConfig.cfg.

=head1 DESCRIPTION

The available arguments for the script are:
        -i repo_id (repository id)
	[-t type_of_list (default: records) || (identifiers)]
	[-s set_id (other than what is in config)] 
	[-m metadata_format (default oai_dc)]
	[-f from (YYYY-MM-DDThh:mm:ssZ)]
	[-u until (YYYY-MM-DDThh:mm:ssZ)]
	[-x max_request (default 10000)]
	[-w (write individual record files)]
	[-d data_dir]
	[-v (verbose)]
	[-r (to resume using token file)


The only required argument is the repository id (-i).

To retrieve all oai_dc records from the repository, run:
    >./OaiList.pl -i $REPOSITORY_ID -s $SET -m oai_dc 
To do ListIndentifiers for all marc21 records from the repository updated since October 1, run:
    >./OaiList.pl -i $REPOSITORY_ID -t identifiers -s $SET -m marc21 -f 2007-10-01
To do ListRecords for all marc21 records and write each record to a single file run:
    >./OaiList.pl -i $REPOSITORY_ID -m marc21 -w


See the Perldoc for updateMbooksOai.pl for details about how OaiList.pl can be used to update the oai table.

Below is an example RepositoryConfig.cgf file:
<?xml version="1.0" encoding="UTF8"?>
<RepositoryConfig>
  <repository id="repo1">
    <baseUrl>http://your.host.edu/repo1/oai</baseUrl>
    <fullName>Repository Foo</fullName>
    <set>foo</set>
  </repository>
  <repository id="repo2">
    <baseUrl>http://your.host.edu/repo1/oai</baseUrl>
    <fullName>Repository Bar</fullName>
    <set>bar</set>
  </repository>
</RepositoryConfig>

