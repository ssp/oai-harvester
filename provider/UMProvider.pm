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

package UMProvider;

use strict;
use POSIX;
use XML::LibXML;
use DBI;
use Encode;


## ----------------------------------------------------------------------------
##  Function:   new() for object
##  Parameters: %hash with a bunch of args (check documentation)
##  Return:     ref to object
## ----------------------------------------------------------------------------
sub new {
    my ($class, %args) = @_;
    my $self = bless {}, $class;

    if ( ! $args{'logFile'} )   { return "missing log file in new()"; }
    if ( ! $args{'url'} )       { return "missing url in new()"; }
    if ( ! $args{'arguments'} ) { return "missing arguments in new()"; }

    $self->InitLogFile(   $args{'logFile'}   );
    $self->SetRequestUrl( $args{'url'}       );
    $self->SetArguments(  $args{'arguments'} );

    if ( ! $self->LoadConfig( $args{'configFile'} ) )
    { 
        return "failed to load config file"; 
    }

    ## optional args
    if ( $args{'maxItems'} ne "" ) { $self->SetMaxItems( $args{'maxItems'} ); }
    else                           { $self->SetMaxItems( "100" ); }

    if ( $args{'tableName'} ne "" ) { $self->SetTableName( $args{'tableName'} ); }
    else                            { $self->SetTableName( "oai" ); }

    if ( $args{'setTableName'} ne "" ) { $self->SetSetsTableName( $args{'setTableName'} ); }
    else                               { $self->SetSetsTableName( "oaisets" ); }

    if ( $args{'DBDriver'} ne "" ) { $self->SetDBDriver( $args{'DBDriver'} ); }
    else                           { $self->SetDBDriver( "mysql" ); }

    if ( $args{'DbUpdate'} ne "" ) { $self->SetDbUpdateTime( $args{'DbUpdate'} ); }

    if ( $args{'shortDate'} ne "" ) { $self->SetShortDate( 1 ); }

    if ( $args{'aboutEnabled'} ne "" ) { $self->SetAboutEnabled( $args{'aboutEnabled'} ); }
    else                               { $self->SetAboutEnabled( 0 ); }

    ## looks good, start the response document
    $self->init();

    $self;
}

## ----------------------------------------------------------------------------
##  Function:   init the object and set the in params, create object XML doc
##  Parameters: nothing, looks at what was set in new
##  Return:     nothing
## ----------------------------------------------------------------------------
sub init
{
    my $self = shift;

    my $url  = $self->GetRequestUrl( "url" );;
    my %args = $self->GetArguments( );

    my $doc          = XML::LibXML::Document->new( "1.0", "UTF-8" );
    my $root         = $doc->createElement( "OAI-PMH" );
    my $responseDate = $doc->createElement( "responseDate" );
    my $request      = $doc->createElement( "request" );

    ## $root->setAttributeNS("", "xmlns", "http://www.openarchives.org/OAI/2.0/");
    $root->setAttribute("xmlns:", "http://www.openarchives.org/OAI/2.0/");
    $root->setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
    $root->setAttribute("xsi:schemaLocation", "http://www.openarchives.org/OAI/2.0/ ".
      "http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd");

    $root->appendChild( $responseDate );
    $root->appendChild( $request );
    $doc->setDocumentElement( $root );

    $responseDate->appendText( $self->GetDateString() );
    $request->appendText( $url );

    foreach my $arg ( keys %args )
    {
        $request->setAttribute( $arg, $args{$arg} );

        if ($arg eq "verb")           { $self->SetVerb( $args{$arg} ); }
        if ($arg eq "until")          { $self->SetUntil( $args{$arg} ); }
        if ($arg eq "from")           { $self->SetFrom( $args{$arg} ); }
        if ($arg eq "identifier")     { $self->SetIdentifier( $args{$arg} ); }
        if ($arg eq "metadataPrefix") { $self->SetMdFormat( $args{$arg} ); }
        if ($arg eq "set")            { $self->SetOaiSet( $args{$arg} ); }

        if ($arg eq "resumptionToken") 
        { 
            my ($tokenTime, $tokenFormat, $tokenFrom, $tokenUntil, $tokenOffset, $tokenSet) = 
              $self->SplitToken( $args{$arg} );

            $self->SetToken      ( $args{$arg} ); 
            $self->SetTokenOffset( $tokenOffset );
            $self->SetTokenTime  ( $tokenTime );
            $self->SetMdFormat   ( $tokenFormat );
            $self->SetFrom       ( $tokenFrom );
            $self->SetUntil      ( $tokenUntil );
            $self->SetOaiSet     ( $tokenSet );
        }
    }
    $self->SetDocument( $doc );
}

## ----------------------------------------------------------------------------
##  Function:   call the right method to build the response (or error)
##  Parameters: nothing
##  Return:     nothing -- added data to XML::LibXML doc
## ----------------------------------------------------------------------------
sub BuildResponse
{
    my $self = shift;

    ## check the verb args
    ## if ( ! $self->ValidateAgruments() ) { $self->AddError( "badArgument", "" ); return; }
    if ( ! $self->ValidateAgruments() ) { return; }
    
    if    ( $self->GetVerb() eq "Identify" )            { $self->Identify(); }
    elsif ( $self->GetVerb() eq "ListSets" )            { $self->ListSets(); }
    elsif ( $self->GetVerb() eq "ListMetadataFormats" ) { $self->ListMetadataFormats(); }
    elsif ( $self->GetVerb() eq "GetRecord" )           { $self->GetRecord(); }
    elsif ( $self->GetVerb() eq "ListIdentifiers" )     { $self->ListIdentifiers(); }
    elsif ( $self->GetVerb() eq "ListRecords" )         { $self->ListRecords(); }
    else                                                { $self->AddError( "badVerb", "" ); }

}

## ----------------------------------------------------------------------------
##  Function:   validate the URL arguemnts
##  Parameters: nothing
##  Return:     1 || 0 -- adds error node to doc is not valid
## ----------------------------------------------------------------------------
sub ValidateAgruments
{
    my $self = shift;
    my %args = $self->GetArguments();
    my $verb = $args{'verb'};
    delete $args{'verb'};

    ## check for double arguments
    foreach ( keys %args )
    {
        if ( $args{$_} =~ /$;/ ) 
        {
            $self->AddError( "badArgument", "" );
            return 0;
        }
    }

    ## make sure the metadataPrefix is not: id, timstamp or about
    ## these are reserved for other parts of the table and should
    ## never be valid metadata format names
    if ( $args{'metadataPrefix'} eq "id" || 
         $args{'metadataPrefix'} eq "timestamp" || 
         $args{'metadataPrefix'} eq "about" )
    {
        $self->AddError( "badArgument", "" );
        return 0;
    }

    ## deal with resumptionToken exclusivity
    if ( $args{'resumptionToken'} )
    {
        if ( keys %args > 1 || $verb eq "GetRecord" || $verb eq "ListMetadataFormats" ) 
        { 
            $self->AddError( "badArgument", "" );
            return 0 
        }
        else { return 1; }
    } 

    ## if no sets table and set is an arg, always an error
    if ( ( $args{'set'} ne "" ) && ( ! $self->CheckForSetsTable() ) ) 
    { 
        $self->AddError( "badArgument", "" );
        return 0; 
    }
 
    if ( (($verb eq "Identify") && (keys %args >= 1)) || 
         (($verb eq "ListSets") && (keys %args >= 1)) )
    {
        $self->AddError( "badArgument", "" );
        return 0; 
    }

    if ( $verb eq "GetRecord" )
    {
        if ( keys %args != 2 ) { $self->AddError( "badArgument", "" ); return 0; }

        if ( $args{'identifier'} )
        {
            if ( ! $self->ValidateIdentifier( $args{'identifier'} ) ) { return 0; }
        }
        else
        {
            $self->AddError( "badArgument", "Missing required arguments: identifier" );
            return 0; 
        }

        if ( ! $args{'metadataPrefix'} )
        {
            $self->AddError( "badArgument", "Missing required arguments: metadataPrefix" );
            return 0; 
        }
    }

    if ( $verb eq "ListMetadataFormats" )
    {
        if ( $args{'identifier'} )
        {
            if ( ! $self->ValidateIdentifier( $args{'identifier'} ) ) 
            {
                $self->AddError( "badArgument", "illegal syntax for: identifier" );
                return 0; 
            }
            delete $args{'identifier'};
        }

        if ( keys %args > 0 ) 
        { 
            $self->AddError( "badArgument", "" );
            return 0; 
        }
    }

    if ( $verb eq "ListIdentifiers" || $verb eq "ListRecords" )
    {
        if ( ! $args{'metadataPrefix'} ) 
        {
            $self->AddError( "badArgument", "Missing required arguments: metadataPrefix" ); 
            return 0; 
        }
        delete $args{'metadataPrefix'};

        if ( ($args{'until'}) && (! $self->ValidateDate( $args{'until'} )) ) { return; }
        if ( ($args{'from'})  && (! $self->ValidateDate( $args{'from'} )) )  { return; }
        
        delete $args{'until'};
        delete $args{'from'};
        delete $args{'set'};
        if ( keys %args > 0 ) { $self->AddError( "badArgument", "" ); return 0; }
    }

    return 1;
}

## ----------------------------------------------------------------------------
##  Function:   validate the identifier arguments
##  Parameters: STRING identifier
##  Return:     1|0 adds error to response if needed
## ----------------------------------------------------------------------------
##    uric        = reserved | unreserved | escaped
##    reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" | "$" | ","
##    unreserved  = alphanum | mark
##    mark        = "-" | "_" | "." | "!" | "~" | "*" | "'" | "(" | ")"
##    escaped     = "%" hex hex
##    hex         = digit | "A" | "B" | "C" | "D" | "E" | "F"
## ----------------------------------------------------------------------------
sub ValidateIdentifier
{
    my $self = shift;
    my $id   = shift;

    if ( $id !~ /^[a-z0-9:;\/\?@&=\+\$\,\-_\.!~\*'\(\)%]+$/i )
    {
        $self->AddError( "badArgument", "illegal syntax for: identifier" );
        return 0;
    }

    return 1;
}

## ----------------------------------------------------------------------------
##  Function:   validate the date in from or util arguments
##  Parameters: STRING date
##  Return:     1|0 adds error to response if needed
## ----------------------------------------------------------------------------
sub ValidateDate
{
    my $self = shift;
    my $date = shift;

    if ( $self->GetShortDate() )
    {
        ## YYYY-MM-DD
        if ( $date !~ m/^\d{4}\-\d{2}\-\d{2}$/ )
        {
            $self->AddError( "badArgument", "illegal syntax for: date (YYYY-MM-DD)" );
            return 0;
        }
    }
    else 
    {
        ## YYYY-MM-DDTHH:MM:SSZ
        if ( $date !~ m/^\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}Z$/ ) 
        {
            $self->AddError( "badArgument", "illegal syntax for: date (YYYY-MM-DDTHH:MM:SSZ)" );
            return 0;
        }
    }
    return 1;
}

## ----------------------------------------------------------------------------
##  Function:   toString() for the object XML
##  Parameters: nothing
##  Return:     STRING -- calls XML:LibXML toString() on object doc
## ----------------------------------------------------------------------------
sub toString
{
    my $self = shift;
    return $self->GetDocument()->toString(1);
}

## ----------------------------------------------------------------------------
## the OAI verbs
##
## ----------------------------------------------------------------------------

## ----------------------------------------------------------------------------
##  Function:   The first 3 oai vers mainly just pull the data from the XML
##              config file.  Not much is dynamic there.
##  Parameters: nothing -- data from object and XML config
##  Return:     nothing -- adds data to the object XML::LibXML::Document
## ----------------------------------------------------------------------------
sub Identify 
{
    my $self = shift;

    my $source = $self->GetConfigSource();
    my ($Identify) = $source->findnodes("/oai_config/Identify");

    if ( ! defined $Identify ) 
    { 
        $self->WriteToLog( "Identify not in config" );
        $self->AddError( "badArgument", "");
        return;
    }

    my ($earliestDatestamp) = $source->findnodes( "/oai_config/Identify/earliestDatestamp" );
    if ( defined $earliestDatestamp ) 
    { 
        $earliestDatestamp->appendText( $self->GetEarliestDatestamp() ); 
    }

    $self->GetDocument()->documentElement()->appendChild( $Identify );
}

sub ListSets 
{
    my $self = shift;
    my $source = $self->GetConfigSource();
    my ($ListSets) = $source->findnodes("/oai_config/ListSets");

    if ( ! defined $ListSets || ! $self->CheckForSetsTable() )
    { 
        $self->AddError( "noSetHierarchy", "");
        return;
    }

    $self->GetDocument()->documentElement()->appendChild( $ListSets );
}

sub ListMetadataFormats 
{
    my $self = shift;
    my $source = $self->GetConfigSource();
    my $table  = $self->GetTableName();
    my ($PossibleMetadataFormats) = $source->findnodes("/oai_config/PossibleMetadataFormats");

    if ( ! defined $PossibleMetadataFormats ) 
    { 
        $self->WriteToLog( "PossibleMetadataFormats not in config" );
        $self->AddError( "noMetadataFormats", "");
        return;
    }

    my $ListMetadataFormats = XML::LibXML::Element->new( "ListMetadataFormats" );

    my @parts = split( ":", $self->GetIdentifier() );
    my $ident = $parts[-1];
    
    # If there are four parts, that means this is an article identifier with an extra colon
    # In this case, get the two rightmost parts and concatenate them
    if( (scalar( @parts )) eq 4) 
    {
	    $ident  = $parts[-2] . ":"  . $ident;
    }   
    
    if ( $ident ne "" )
    {
        my $foundOne = 0;
        foreach my $mdFormat ( $source->findnodes("/oai_config/PossibleMetadataFormats/metadataFormat") )
        {
            my $format = $mdFormat->findvalue( "metadataPrefix" );
            my $sql    = "SELECT count(id) from $table WHERE (id = \"$ident\" AND $format IS NOT NULL)";
            if ( scalar( $self->GetDbHandle->selectrow_array( $sql ) ) )
            {
                $foundOne++;
                $ListMetadataFormats->appendChild( $mdFormat );
            }
        }
        if ( ! $foundOne ) { $self->AddError( "idDoesNotExist", ""); return; }
    }
    else
    {
        foreach my $mdFormat ( $source->findnodes("/oai_config/PossibleMetadataFormats/metadataFormat") )
        {
            ## add them all, don't check
            $ListMetadataFormats->appendChild( $mdFormat );
        }
    }

    $self->GetDocument()->documentElement()->appendChild( $ListMetadataFormats );
}

sub GetRecord 
{
    my $self = shift;
    
    my $GetRecord = XML::LibXML::Element->new( "GetRecord" );
    my $record    = XML::LibXML::Element->new( "record" );
    my $table     = $self->GetTableName();

    my $format    = $self->GetMdFormat();
    my @parts     = split( ":", $self->GetIdentifier() );
    my $ident     = $parts[-1];
    
    # If there are four parts, that means this is an article identifier with an extra colon
    # In this case, get the two rightmost parts and concatenate them
    if( (scalar( @parts )) eq 4) 
    {
	    $ident  = $parts[-2] . ":"  . $ident;
    }

    my $deleted   = $self->DeletionCheck( $ident, $format );
    my $metadata  = $self->GetMetadataRecord( $ident, $format );
    my $about     = $self->GetAboutRecord( $ident );

    if ( $ident eq "" || $format eq "" ) { $self->AddError( "badArgument", ""); return; }

    if ( $metadata ne "" || $deleted ) 
    { 
        my ($id, $time, $set) = $self->GetDbItem( $ident );
        my $header = $self->BuildHeader($id, $format, $time, $set, 0, $deleted);
        if ( ! defined $header )
        {
            $self->WriteToLog("failed to build header for: $id, $time, $set");
        }
        else { $record->appendChild( $header ); }

        if ( $metadata  && (! $deleted) ) { $record->appendChild( $metadata ); }
        if ( $about     && (! $deleted) ) { $record->appendChild( $about );    }
        $GetRecord->appendChild( $record );
        $self->GetDocument()->documentElement()->appendChild( $GetRecord );
    }

    else { $self->AddError( "idDoesNotExist", ""); return; }
}

sub ListIdentifiers 
{
    my $self = shift;

    my $ListIdentifiers = XML::LibXML::Element->new( "ListIdentifiers" );
 
    my $token  = $self->GetToken();
    my $format = $self->GetMdFormat();
    if ( $token ne "" )
    {
        if ( ! $self->ValidateToken( $token ) ) 
        { 
            $self->AddError( "badResumptionToken", "" ); 
            return;
        }
    } 
    else
    { 
        if ( $format eq "" ) { $self->AddError( "badArgument", ""); return; }
    }

    my ($rows, $tokenNode) = $self->GetDbRows( );
    if ( ! $rows || ! scalar( @$rows ) ) { $self->AddError( "noRecordsMatch", "" ); return; }
    foreach my $row ( @$rows )
    {
        my ($id, $time, $set) = @$row;
        my $deleted = $self->DeletionCheck( $id, $format );
        my $header  = $self->BuildHeader($id, $format, $time, $set, 0, $deleted);
        if ( ! defined $header )
        {
            $self->WriteToLog("failed to build header for: $id, $time, $set");
        }
        else 
        {
            $ListIdentifiers->appendChild( $header );
        }
    }

    ## add a token (maybe empty) if we need one
    if ( $tokenNode ) { $ListIdentifiers->appendChild( $tokenNode ); }

    $self->GetDocument()->documentElement()->appendChild( $ListIdentifiers );
}

sub ListRecords
{
    my $self = shift;

    my $ListRecords = XML::LibXML::Element->new( "ListRecords" );

    my $token  = $self->GetToken();
    my $format = $self->GetMdFormat();
    if ( $token ne "" )
    {
        if ( ! $self->ValidateToken( $token ) )
        {
            $self->AddError( "badResumptionToken", "" );
            return;
        }
    }
    else
    {
        if ( $format eq "" ) { $self->BuildError( "badArgument", ""); return; }
    }

    my ($rows, $tokenNode) = $self->GetDbRows( );
    if (! $rows || ! scalar( @$rows) ) { $self->AddError( "noRecordsMatch", "" ); return; }

    my $recordsStr;
    foreach my $row ( @$rows )
    {
        my ($id, $time, $set) = @$row;
        my $deleted   = $self->DeletionCheck( $id, $format );
        my $recordStr = $self->GetMetadataRecordStr( $id, $format );
        $recordStr   .= $self->GetAboutRecordStr( $id );

        $recordsStr .= "<record>\n";
        $recordsStr .= $self->BuildHeader($id, $format, $time, $set, 1, $deleted);
        Encode::_utf8_on($recordStr);   ## force to utf-8 if not already
        $recordsStr .= $recordStr; 
        $recordsStr .= "</record>\n";
    }

    ## now parse the balanced chunk string and add the XML::LibXML node
    my $parser = $self->GetXmlParser();
    my $recordsNode;
    eval { $recordsNode = $parser->parse_balanced_chunk( $recordsStr ); };
    if ( $@ )
    {
        $self->WriteToLog( "failed parse records chunk: $recordsStr: $@" );
        $self->AddError( "noRecordsMatch", "" );
        return;
    }
    $ListRecords->appendChild( $recordsNode );

    ## add a token (maybe empty) if we need one
    if ( $tokenNode ) { $ListRecords->appendChild( $tokenNode ); }

    $self->GetDocument()->documentElement()->appendChild( $ListRecords );
}


## ----------------------------------------------------------------------------
##  Function:   load the OAI config data
##  Parameters: config file
##  Return:     1 || 0
## ----------------------------------------------------------------------------
sub LoadConfig
{
    my $self = shift;
    my $file = shift;

    my $parser = $self->GetXmlParser();
    my $source;
    eval { $source = $parser->parse_file( $file ); };
    if ( $@ ) 
    { 
        $self->WriteToLog( "failed to parse config file $file: $@" );
        return 0;
    }
    
    $self->SetConfigSource( $source );
    
    my $xpath = "/oai_config/Identify/description/*[name()='oai-identifier']/*[name()='repositoryIdentifier']";
    my $repositoryIdentifier = $source->findvalue( $xpath );
    $self->SetProviderHost( $repositoryIdentifier );

    return 1;
}

## ----------------------------------------------------------------------------
##  Function:   get the date in given format (DB, OAI, resumptionToken)
##  Parameters: [date], [type] (default is OAI format)
##  Return:     STRING -- date 
##              OAI: YYYY-MM-DDTHH:MM:SSZ
##              DB:  YYYY-MM-DD HH:MM:SS
##              RT:  YYYYMMDDHHMMSS
## ----------------------------------------------------------------------------
sub GetDateString
{
    my $self = shift;
    my $date = shift;
    my $type = shift;

    if ( ! $date ) { $date = POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime(time)); }

    my @t = $date =~ m/(\d{4})\-?(\d{2})\-?(\d{2})T?\s?(\d{2})?:?(\d{2})?:?(\d{2})?/;

    if ( $self->GetShortDate() )
    {
        if    ( $type eq "db" ) { return "$t[0]-$t[1]-$t[2] 00:00:00"; }
        elsif ( $type eq "rt" ) { return "$t[0]$t[1]$t[2]"; }
        else                    { return "$t[0]-$t[1]-$t[2]"; }
    }
    else 
    {
        if    ( $type eq "db" ) { return "$t[0]-$t[1]-$t[2] $t[3]:$t[4]:$t[5]"; }
        elsif ( $type eq "rt" ) { return "$t[0]$t[1]$t[2]$t[3]$t[4]$t[5]"; }
        else                    { return "$t[0]-$t[1]-$t[2]T$t[3]:$t[4]:$t[5]Z"; }
    }
}

## ----------------------------------------------------------------------------
##  Function:   Get the oldest date from the DB for OAI spec
##  Parameters: nothing (gets oldest date from DB)
##  Return:     STRING -- date YYYY-MM-DDTHH:MM:SS
## ----------------------------------------------------------------------------
sub GetEarliestDatestamp
{
    my $self = shift;

    ## if we are not connected to the DB, just return a cheap default
    if ( ! defined $self->GetDbHandle ) { return "1990-01-01T01:01:01Z"; }

    my $table = $self->GetTableName();
    my $sql   = "SELECT timestamp from $table ORDER by timestamp ASC LIMIT 1";
    my $res   = $self->GetDbHandle->selectall_arrayref( $sql );
    my $date  = $res->[0][0];
    $date     = $self->GetDateString( $date, "" );

    return $date;
}

## ----------------------------------------------------------------------------
##  Function:   build the OAI header from DB data
##  Parameters: ID, time, oai set, [string]
##  Return:     ref to XML::LibXML::Node 
## ----------------------------------------------------------------------------
sub BuildHeader
{
    my $self   = shift;
    my $id     = shift;
    my $format = shift;
    my $time   = shift;
    my $set    = shift;
    my $str    = shift;
    my $del    = shift;

    $time = $self->GetDateString( $time, "" );
    
    my $header     = XML::LibXML::Element->new( "header" );
    my $identifier = XML::LibXML::Element->new( "identifier" );
    my $datestamp  = XML::LibXML::Element->new( "datestamp" );


    $identifier->appendText( "oai:" . $self->GetProviderHost() . ":" . $id );
    $datestamp->appendText( $time );

    $header->appendChild( $identifier );
    $header->appendChild( $datestamp );

    foreach ( split($;, $set) )
    {
        my $setSpec     = XML::LibXML::Element->new( "setSpec");
        ## my $fullSetName = $self->GetFullSetName( $_ );
        my $fullSetName = $_;
        $setSpec->appendText( $fullSetName );
        $header->appendChild( $setSpec );
    }

    ## If format is "", record has been deleted
    if ( $del ) { $header->setAttribute( "status", "deleted" ); }

    ## return the string, not XML::LibXML node
    if ($str) { return $header->toString(); }

    return $header;
}

## ----------------------------------------------------------------------------
##  Function:   get the metadata or a given ID and format.  Also, this is for 
##                GetRecord, not ListRecords -- The logic is a little different
##  Parameters: ID, format (oai_dc, marc21...)
##  Return:     reference to a XML::LibXML::Node 
## ----------------------------------------------------------------------------
sub GetMetadataRecord
{
    my $self   = shift;
    my $ident  = shift;
    my $format = shift;
    my $parser = $self->GetXmlParser();

    my $recordStr = $self->GetMetadataRecordStr( $ident, $format );
 
    if ( $recordStr eq "" ) { return; }

    my $node;
    eval { $node = $parser->parse_balanced_chunk( $recordStr ); };
    if ( $@ ) 
    { 
        $self->WriteToLog( "failed parse chunk for: $ident: $@" );
        $self->AddError( "noRecordsMatch", "" );
    } 

    return $node;
}

## ----------------------------------------------------------------------------
##  Function:   get the metadata (string) or a given ID and format
##  Parameters: ID, format (oai_dc, marc21...)
##  Return:     STRING -- string of data directly from the DB BLOB
## ----------------------------------------------------------------------------
sub GetMetadataRecordStr
{
    my $self   = shift;
    my $ident  = shift;
    my $format = shift;
    
    if ( $self->DeletionCheck( $ident, $format ) ) { return; }

    my $table = $self->GetTableName();

    ## check to see if the format column exists
    my $sth  = $self->GetDbHandle->column_info( undef, undef, $table, '%' );
    my $ref  = $sth->fetchall_arrayref;
    my @cols = map { $_->[3] } @$ref;
    if ( ! grep(/^$format$/, @cols) )  { $self->AddError( "cannotDisseminateFormat", ""); return; }

    my $sql       = "SELECT $format FROM $table WHERE id = \"$ident\"";
    my $res       = $self->GetDbHandle->selectall_arrayref( $sql );
    my $recordStr = $res->[0][0];

    ## hack to work around old libxml problem...
    my ($libmaj, $libmin, $librev) = split(/\./, XML::LibXML::LIBXML_DOTTED_VERSION );
    if ( ($libmin <= 6) && ($librev < 26) )
    {
        Encode::from_to( $recordStr, "iso-8859-1", "utf8" );
    }

    if ( $recordStr eq "" )
    {
        ## $self->WriteToLog( "no data in DB for: $ident" );
        ## $self->AddError( "noRecordsMatch", "for $ident" );
    }

    return $recordStr;
}

## ----------------------------------------------------------------------------
##  Function:   get the <about> for a given ID if we have one. 
##  Parameters: ID,
##  Return:     reference to a XML::LibXML::Node 
## ----------------------------------------------------------------------------
sub GetAboutRecord
{
    my $self   = shift;
    my $ident  = shift;

    if ( ! $self->GetAboutEnabled() ) { return; }

    my $aboutStr = $self->GetAboutRecordStr( $ident );

    if ( $aboutStr eq "" ) { return; }

    ## hack to work around old libxml problem...
    my ($libmaj, $libmin, $librev) = split(/\./, XML::LibXML::LIBXML_DOTTED_VERSION );
    if ( ($libmin <= 6) && ($librev < 26) )
    {
        Encode::from_to( $aboutStr, "iso-8859-1", "utf8" );
    }

    my $parser = $self->GetXmlParser();

    my $node;
    eval { $node = $parser->parse_balanced_chunk( $aboutStr ); };
    if ( $@ )
    {
        $self->WriteToLog( "failed parse chunk for: $ident: $@" );
        $self->AddError( "noRecordsMatch", "" );
    }

    return $node;
}

## ----------------------------------------------------------------------------
##  Function:   get the <about> for a given ID if exists 
##  Parameters: ID
##  Return:     STRING (XML chunk) 
## ----------------------------------------------------------------------------
sub GetAboutRecordStr
{
    my $self   = shift;
    my $ident  = shift;

    if ( ! $self->GetAboutEnabled() )    { return; }
    if ( ! $self->CheckForId( $ident ) ) { return; }

    ## check to see if the about column exists
    my $table = $self->GetTableName();
    my $sql   = "SELECT about FROM $table WHERE id = \"$ident\"";
    my $res   = $self->GetDbHandle->selectall_arrayref( $sql );
    if ( ! scalar @$res ) { return; }

    my $aboutStr = $res->[0][0];

    return $aboutStr;
}

## ----------------------------------------------------------------------------
##  Function:   connect to the DB
##  Parameters: user, passwd, DB name, DB server
##  Return:     nothing, sets the DB handle in the object
## ----------------------------------------------------------------------------
sub ConnectToDb
{
    my $self      = shift;
    my $dbUser    = shift;
    my $dbPasswd  = shift;
    my $dbName    = shift;
    my $dbServer  = shift;
    my $dbDriver  = $self->GetDBDriver();

    my $dbh = DBI->connect( "DBI:$dbDriver:$dbName:$dbServer", $dbUser, $dbPasswd,
      { RaiseError => 1, AutoCommit => 0 } ) || return 0;
    $self->SetDbHandle( $dbh );
}

sub DisconnectDb
{
    my $self = shift;
    if ( defined $self->GetDbHandle ) { $self->GetDbHandle->disconnect(); }
}


## ----------------------------------------------------------------------------
##  Function:   Get the update time from the OAI table
##  Parameters: nothing
##  Return:     timestamp
## ----------------------------------------------------------------------------
sub GetDbUpdateTime 
{ 
    my $self = shift; 
    if ( $self->{'dbUpdate'} ) { return $self->{'dbUpdate'}; }

    my $table = $self->GetTableName();
    my $sql   = "SHOW TABLE STATUS LIKE \"$table\"";

    my $res = $self->GetDbHandle->selectall_arrayref( $sql );    
    if ( ! scalar @$res ) { return; }    
    $self->SetDbUpdateTime( $res->[0]->[12] );

    return $res->[0]->[12];
}

sub SetDbUpdateTime 
{ 
    my $self = shift; 
    $self->{'dbUpdate'} = shift; 
}

## ----------------------------------------------------------------------------
##  Function:   get all of the DB rows (not 
##  Parameters: ID
##  Return:     ID, timestamp, set(s) [, sep. list]
## ----------------------------------------------------------------------------
sub GetDbItem
{
    my $self  = shift;
    my $ident = shift;
    my $table = $self->GetTableName();

    if ( $ident eq "" ) { return; }

    my $sql = "SELECT id, timestamp FROM $table WHERE id = \"$ident\"";
    my $res = $self->GetDbHandle->selectall_arrayref( $sql );
    if ( ! scalar @$res ) { return; }

    ## get the set(s)
    my $sets = $self->GetSets( $res->[0]->[0] );

    return $res->[0]->[0], $res->[0]->[1], $sets;
}

## ----------------------------------------------------------------------------
##  Function:   check to see if there is a oaiset table
##  Parameters: nothing
##  Return:     boolean
## ----------------------------------------------------------------------------
sub CheckForSetsTable
{
    my $self  = shift;
    my $table = $self->GetSetsTableName();

    if ( ( defined $self->{'setTableExists'} ) &&
         ( ! $self->{'setTableExists'} ) ) { return 0; }

    my $res = $self->GetDbHandle->selectall_arrayref( "SHOW TABLES LIKE \"$table\"" );
    if ( ! scalar( @$res ) ) { $self->{'setTableExists'} = 0; }
    else                     { $self->{'setTableExists'} = 1; }

    return $self->{'setTableExists'};
}

## ----------------------------------------------------------------------------e
##  Function:   get all sets for a given ID
##  Parameters: ID 
##  Return:     set(s) [, sep. list]
## ----------------------------------------------------------------------------
sub GetSets
{
    my $self  = shift;
    my $ident = shift;
    my $table = $self->GetSetsTableName();
        
    if ( $ident eq "" )                 { return; }
    if ( ! $self->CheckForSetsTable() ) { return; }

    my $sql = "SELECT oaiset FROM $table WHERE id = \"$ident\"";
    my $res = $self->GetDbHandle->selectall_arrayref( $sql ); 

    if ( ! scalar @$res ) { return; }

    my @sets;
    foreach ( @$res ) { push @sets, $_->[0]; }

    my @sortedSet = sort {length $b <=> length $a} @sets;
    my @dedupedSets;
    foreach my $set ( @sortedSet ) 
    { 
        if ( ! grep {/^$set/} @dedupedSets ) { push @dedupedSets, $set; }
    }

    return join($;, @dedupedSets);
}

## ----------------------------------------------------------------------------
##  Function:   get the rows in the DB based on object paramaters
##  Parameters: nothing -- looks at object
##  Return:     ref to anon array $r->[] (not BLOB rows), token string
## ----------------------------------------------------------------------------
sub GetDbRows
{
    my $self      = shift;
    my $from      = $self->GetFrom();
    my $until     = $self->GetUntil();
    my $token     = $self->GetToken();
    my $format    = $self->GetMdFormat();
    my $offset    = $self->GetTokenOffset();
    my $oaiSet    = $self->GetOaiSet();
    my $table     = $self->GetTableName();
    my $setsTable = $self->GetSetsTableName();
    my $maxItems  = $self->GetMaxItems();

    if ( $from   ne "" ) { $from   = $self->GetDateString( $from,  "db" ); }
    if ( $until  ne "" ) { $until  = $self->GetDateString( $until, "db" ); }
    if ( $offset eq "" ) { $offset = 0; }  ## default 0, not null

    ## check to see if the format column exists
    my $sth  = $self->GetDbHandle->column_info( undef, undef, $table, '%' );
    my $ref  = $sth->fetchall_arrayref;
    my @cols = map { $_->[3] } @$ref;
    if ( ! grep(/^$format$/, @cols) )  { $self->AddError( "cannotDisseminateFormat", ""); return; }

    ## start buildig the SQL query
    my $sql = "SELECT SQL_CALC_FOUND_ROWS $table.id, $table.timestamp FROM $table "; 

    if ( $oaiSet ne "" ) { $sql .= " LEFT JOIN $setsTable USING (id) " }

    ## if oai_dc is NULL, we still want the id -- these are deleted records
    my @whereList;
    if ( $format ne "oai_dc" ) { push @whereList, "$format IS NOT NULL"; }

    if ($from ne "" )  { push @whereList, "timestamp >= \"$from\" "; }
    if ($until ne "" ) { push @whereList, "timestamp <= \"$until\" "; }

    if ($oaiSet ne "") 
    { 
        push @whereList, "($table.id = $setsTable.id AND $setsTable.oaiset = \"$oaiSet\")"; 
    }

    if (scalar @whereList) { $sql .= " WHERE " . join(" AND ", @whereList); }

    $sql .= " ORDER BY timestamp ASC, id ASC LIMIT $offset, $maxItems";

    ## now get the rows and row count without LIMIT
    my $allRows    = $self->GetDbHandle->selectall_arrayref( $sql );
    my $foundRows  = $self->GetDbHandle->selectall_arrayref( "SELECT FOUND_ROWS()" );

    ## get all oaisets for the found IDs
    foreach my $row ( @$allRows ) { $row->[2] = $self->GetSets( $row->[0] ); }

    my $resultSize = scalar( @$allRows );
    $self->SetListSize( $foundRows->[0]->[0] );

    ## if full size > MAX add a token
    if ( $self->GetListSize() > $self->GetMaxItems() )
    { 
        my $resumptionToken = XML::LibXML::Element->new( "resumptionToken" );
        $resumptionToken->setAttribute( "completeListSize", $self->GetListSize() );

        my $newOffset = ($self->GetMaxItems() + $offset);
        $self->SetTokenOffset( $newOffset);
        if ( $newOffset < $self->GetListSize ) { $self->SetToken( $self->BuildToken() ); }
        else                                   { $self->SetToken( "" ); } 

        $resumptionToken->appendText( $self->GetToken() );
        return ($allRows, $resumptionToken);
    }

    return ($allRows, "");
}

## ----------------------------------------------------------------------------
##  Function:   check to see if an item is in the main table
##  Parameters: identifier
##  Return:     BOOLEAN -- 1 if found, 0 if not
## ----------------------------------------------------------------------------
sub CheckForId
{
    my $self  = shift;
    my $ident = shift;
    my $table = $self->GetTableName();

    my $sql = "SELECT id FROM $table WHERE id = \"$ident\"";
    my $res = $self->GetDbHandle->selectall_arrayref( $sql );
    if ( scalar @$res ) { return 1; }

    return 0;
}

## ----------------------------------------------------------------------------
##  Function:   check to see if an item was deleted
##  Parameters: identifier
##  Return:     BOOLEAN -- 1 if item in DB but was deleted
## ----------------------------------------------------------------------------
sub DeletionCheck
{
    my $self   = shift;
    my $ident  = shift;
    my $format = shift;

    my $table = $self->GetTableName();
    my $sql   = qq{ SELECT id, oai_dc FROM $table WHERE id = "$ident" };
    my $res   = $self->GetDbHandle->selectall_arrayref( $sql );

    ## ID not found
    if ( ! scalar @$res ) { return 0; }

    ## oai_dc should not be "" OR NULL so check to see if NULL
    if ( ! $res->[0][1] ) { return 1; }

    my $sql = qq{ SELECT $format FROM $table WHERE id = "$ident" AND $format = "" };
    my $res = $self->GetDbHandle->selectall_arrayref( $sql );

    ## format is "" (deleted), not to be confsed with NULL (meaning never existed)
    if ( scalar @$res ) { return 1; } ## format is ""

    return 0; 
}

## ----------------------------------------------------------------------------
##  Function:   gets the ref to the XML::LIbXML parser, creates if not defined
##  Parameters: nothing
##  Return:     refernce to XML::LibXML::Parser 
## ----------------------------------------------------------------------------
sub GetXmlParser
{
    my $self = shift;
    if ( ! $self->{'xmlParser'} ) { $self->{'xmlParser'} = XML::LibXML->new(); }
    return $self->{'xmlParser'};
}

## ----------------------------------------------------------------------------
##  Function:   init log file (open log file)
##  Parameters: full path to log file 
##  Return:     nothing, sets file handle in object
## ----------------------------------------------------------------------------
sub InitLogFile
{
    my $self    = shift;
    my $logFile = shift;

    if ( open( my $fh, ">>", $logFile ) )
    {
        $self->{'logFileHandle'} = $fh;
    }
    else { print "failed to open log file: $logFile \n"; }
}


## ----------------------------------------------------------------------------
##  Function:   write a message to the log file (date message)
##  Parameters: STRING -- message
##  Return:     nothing
## ----------------------------------------------------------------------------
sub WriteToLog
{
    my $self    = shift;
    my $message = shift;
    my $fh      = $self->{'logFileHandle'};
    my $date    = $self->GetDateString();
    print $fh $date . " " . $message . "\n";
} 

## ----------------------------------------------------------------------------
##  Function:   validate the token to make sure it's not too old or whatever
##  Parameters: STRING -- token
##  Return:     1 || 0
## ----------------------------------------------------------------------------
sub ValidateToken
{
    my $self  = shift;
    my $token = shift;

    ## too old 
    my ($time, $md, $from, $until, $off, $set) = $self->SplitToken( $token );

    if ( ($self->GetDateString($time, "db") cmp $self->GetDbUpdateTime()) != 1 ) { return 0; }

    return 1;
}

## ----------------------------------------------------------------------------
##  Function:   split up the token into different parts
##  Parameters: STRING -- token
##  Return:     (time, format, from, until, offset, set)
## ----------------------------------------------------------------------------
sub SplitToken
{
    my $self  = shift;
    my $token = shift;
 
    my ($t, $m, $f, $u, $o, $s) = split( ":", $token, 6);
    return ($t, $m, $f, $u, $o, $s);
}

## ----------------------------------------------------------------------------
##  Function:   build a token in the form of
##              time:format:until:id
##              1189717489:oai_dc:2007-10-03:MIU01-000000132
##  Parameters: (ID, [time], [until date], [md format], listSize
##  Return:     STRING 
## ----------------------------------------------------------------------------
sub BuildToken
{
    my $self  = shift;

    my $until = $self->GetUntil();
    my $from  = $self->GetFrom();
    my $md    = $self->GetMdFormat();
    my $set   = $self->GetOaiSet();
    my $time  = $self->GetTokenTime();
    my $off   = $self->GetTokenOffset();

    if ( ! $time ) { $time  = $self->GetDateString( "", "rt" ); }
    if ( $until )  { $until = $self->GetDateString( $until, "rt" ); }
    if ( $from )   { $from  = $self->GetDateString( $from, "rt" ); }

    return "$time:$md:$from:$until:$off:$set";
}


## ----------------------------------------------------------------------------
##  Function:   a bunch of setter and getters
##  Parameters: content you are setting or nothing for getters
##  Return:     something
## ----------------------------------------------------------------------------
sub SetVerb { my $s = shift; $s->{'verb'} = shift; }
sub GetVerb { my $s = shift; $s->{'verb'}; }

sub SetUntil { my $s = shift; $s->{'untilDate'} = shift; }
sub GetUntil { my $s = shift; $s->{'untilDate'}; }

sub SetDocument { my $s = shift; $s->{'document'} = shift; }
sub GetDocument { my $s = shift; $s->{'document'}; }

sub SetTokenOffset { my $s = shift; $s->{'offset'} = shift; }
sub GetTokenOffset { my $s = shift; $s->{'offset'}; }

sub SetTokenTime { my $s = shift; $s->{'tokenTime'} = shift; }
sub GetTokenTime { my $s = shift; $s->{'tokenTime'}; }

sub SetListSize { my $s = shift; $s->{'listSize'} = shift; }
sub GetListSize { my $s = shift; $s->{'listSize'}; }

sub SetFrom { my $s = shift; $s->{'fromDate'} = shift; }
sub GetFrom { my $s = shift; $s->{'fromDate'}; }

sub SetOaiSet { my $s = shift; $s->{'oaiSet'} = shift; }
sub GetOaiSet { my $s = shift; $s->{'oaiSet'}; }

sub SetTableName{ my $s = shift; $s->{'tableName'} = shift; }
sub GetTableName{ my $s = shift; $s->{'tableName'}; }

sub SetSetsTableName{ my $s = shift; $s->{'setsTableName'} = shift; }
sub GetSetsTableName{ my $s = shift; $s->{'setsTableName'}; }

sub SetDbHandle{ my $s = shift; $s->{'dbHandle'} = shift; }
sub GetDbHandle{ my $s = shift; $s->{'dbHandle'}; }

sub SetDataDir { my $s = shift; $s->{'dataDir'} = shift; }
sub GetDataDir { my $s = shift; $s->{'dataDir'}; }

sub SetConfigSource { my $s = shift; $s->{'configSource'} = shift; }
sub GetConfigSource { my $s = shift; $s->{'configSource'}; }

sub SetRequestUrl { my $s = shift; $s->{'requestUrl'} = shift; }
sub GetRequestUrl { my $s = shift; $s->{'requestUrl'}; }

sub SetProviderHost { my $s = shift; $s->{'provierhost'} = shift; }
sub GetProviderHost { my $s = shift; $s->{'provierhost'} }

sub SetArguments { my $s = shift; $s->{'requestArgs'} = shift; }
sub GetArguments { my $s = shift; %{$s->{'requestArgs'}}; }

sub SetMaxItems { my $s = shift; $s->{'maxItems'} = shift; }
sub GetMaxItems { my $s = shift; $s->{'maxItems'}; }

sub SetToken { my $s = shift; $s->{'resumptionToken'} = shift; }
sub GetToken { my $s = shift; $s->{'resumptionToken'}; }

sub SetMdFormat { my $s = shift; $s->{'metadataFormat'} = shift; }
sub GetMdFormat { my $s = shift; $s->{'metadataFormat'};}

sub SetIdentifier { my $s = shift; $s->{'identifier'} = shift; }
sub GetIdentifier { my $s = shift; $s->{'identifier'}; }

sub SetDBDriver { my $s = shift; $s->{'dbdriver'} = shift; }
sub GetDBDriver { my $s = shift; $s->{'dbdriver'}; }

sub SetShortDate { my $s = shift; $s->{'shortDate'} = shift; }
sub GetShortDate { my $s = shift; $s->{'shortDate'}; }

sub SetAboutEnabled { my $s = shift; $s->{'aboutenabled'} = shift; }
sub GetAboutEnabled { my $s = shift; $s->{'aboutenabled'}; }

## ----------------------------------------------------------------------------
##  ERROR HANDLING
## ----------------------------------------------------------------------------
## ----------------------------------------------------------------------------
##  Function:   build an error node
##  Parameters: code [message], default message is used if none provided
##  Return:     libxml <error> node
## ----------------------------------------------------------------------------
sub BuildError
{
    my $self    = shift;
    my $code    = shift;
    my $message = shift;

    if ( ! $message ) { $message = $self->GetErrorMessage( $code ); }
    my $error = XML::LibXML::Element->new( "error" );
    $error->setAttribute( "code", $code );
    $error->appendText( $message );

    return $error;
}

## ----------------------------------------------------------------------------
##  Function:   add an error node to the document
##  Parameters: code [message], default message is used if none provided
##  Return:     nothing
## ----------------------------------------------------------------------------
sub AddError
{
    my $self    = shift;
    my $code    = shift;
    my $message = shift;

    $self->GetDocument()->documentElement()->appendChild( 
      $self->BuildError( $code, $message ) );

    ## FROM THE OAI-PMH spec: In cases where the request that generated this 
    ##   response resulted in a badVerb or badArgument error condition, the 
    ##   repository must return the base URL of the protocol request only. 
    ##   Attributes must not be provided in these cases."
    if ( $code eq "badVerb" || $code eq "badArgument" ) { $self->RemoveUrlParams(); }

    1;
}

sub RemoveUrlParams
{
    my $self = shift;
    my $doc  = $self->GetDocument()->documentElement();

    my ($requestNode) = $doc->findnodes( "request" );
    foreach my $attrNode ( $requestNode->attributes() ) { $attrNode->unlink(); }
}

## ----------------------------------------------------------------------------
##  Function:   get the default oai error message based on the code
##  Parameters: code
##  Return:     STRING -- error message
## ----------------------------------------------------------------------------
sub GetErrorMessage
{
    my $self = shift;
    my $code = shift;

    my %errors = (
      badArgument => 'The request includes illegal arguments, is missing ' .
                     'required arguments, includes a repeated argument, or ' .
                     'values for arguments have an illegal syntax.',
      badResumptionToken => 'The value of the resumptionToken argument is ' .
                            'invalid or expired.',
      badVerb => 'Value of the verb argument is not a legal OAI-PMH verb, the ' .
                 ' verb argument is missing, or the verb argument is repeated.',
      cannotDisseminateFormat => 'The metadata format identified by the value ' .
                                 'given for the metadataPrefix argument is not ' .
                                 'supported by the item or by the repository.',
      idDoesNotExist => 'The value of the identifier argument is unknown or ' .
                        'illegal in this repository.',
      noRecordsMatch => 'The combination of the values of the from, until, set ' .
                        'and metadataPrefix arguments results in an empty list.',
      noMetadataFormats => 'There are no metadata formats available for the ' .
                           'specified item.',
      noSetHierarchy => 'The repository does not support sets.',
    );

    return $errors{$code}; ## || "No further information available"; 
}

1;

__END__


=head1 NAME

UMProvider -- Perl based OAI-PMH 2.0 Provider

=head1 SYNOPSIS

 use UMProvider;
 use CGI;
 use CGI::Carp;

 my $query  = CGI->new();
 my @params = $query->param();
 my $url    = $query->url();

 my $args = {};
 foreach ( $query->param() ) { my @v = $query->param($_); $args->{$_} = "@v"; }

 my $op = new UMProvider(
     configFile => "oai_provider_conf.xml",
     logFile    => "oai_provider.log",
     url        => $url,
     arguments  => $args);
 
 if ( $op !~ /UMProvider/ )
 {
     carp ($op);
     print $query->header(-status => 500);
     exit;
 }

 if ( ! $op->ConnectToDb( $db_user, $db_passwd, $db_name, $db_server ) )
 {
     carp ("failed ConnectToDb: $db_user, $db_passwd, $db_name, $db_server");
     print $query->header(-status => 500);
     exit;
 }

 $op->BuildResponse();
 print $query->header(-type => 'text/xml', -charset => 'utf-8', -status => 200);
 print $op->toString();


=head1 DESCRIPTION

UMProvider requires that you have pre-formed B<oai_dc> metadata in a database table.  The B<id> field must contain a unique identifier for each record.  The B<timestamp> field should be maintained by the database and will be updated when a change is made to a record.  The B<oai_dc> (or any other format) should be a well formed XML chunk.  The root element for this XML chunk must always be the <metadata> element, normally an immediate child of the <record> element (see the OAI-PMH 2.0 spec for details).

The default database is mysql but any DBI.pm supported database should work.  The data must be stored in two tables with the following required columns:

   +----+-----------+--------+
   | table: oai		     |
   +----+-----------+--------+
   | id | timestamp | oai_dc | 
   +----+-----------+--------+

   +----+-----------+
   | table: oaisets |
   +----+-----------+
   | id | oaiset    |
   +----+-----------+

The second table (oaisets) used to store oai set information is optional just like the use of sets in OAI-PMH 2.0.

If you would like to provide additional metadata formats such as marc21, mods or anything else, just add these columns after oai_dc in the first (main) table.  The column name must match the metadata format.  Here are example create table statements:

  CREATE TABLE oai (id VARCHAR(20) PRIMARY KEY, 
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
    oai_dc MEDIUMBLOB, 
    marc21 MEDIUMBLOB);

  CREATE TABLE oaisets (id VARCHAR(20), oaiset VARCHAR(10);

The "id" values in the "oai" table and the "oaisets" tables must match.  The id in the oai table must be unique but since an item can exist in multiple oai sets that id can repeat in the oaisets table.  Records do not have to be assigned to a set so it is possible that an item in the oai table does not exist in the oaisets table.

If you would like to add <about> containters to your data, add an "about" column to your first table (oai).  This can be done with this command:

  ALTER TABLE oai ADD COLUMN about blob;

   +----+-----------+--------+-------+
   | table: oai		             |
   +----+-----------+--------+-------+
   | id | timestamp | oai_dc | about |
   +----+-----------+--------+-------+


IMPORTANT: If you have hierarchical sets, be sure to have an entry for each unique set name down to the root set for each item.  For example, if the item "abc123" is in the set "foo:bar:baz", you should have the following three rows in the oaisets table:

   +--------+-------------+
   | id     | oaiset      |
   +--------+-------------+
   | abc123 | foo:bar:baz |
   | abc123 | foo:bar     |
   | abc123 | foo         |
   +--------+-------------+

The id in the database is only the brief unique identifier and not the full OAI identifier.  For the OAI response, the identifier is created with the host name (repositoryIdentifier) from the configuration file (oai:host:id).

Each mtadata format for a record can be marked as deleted.  To delete a format for a record, change the data in the field to an empty string ("").  This is not to be confused with NULL witch is the default value for fields.  The UMProvider will continue to return the header for this record but the header will have the "deleted" status attribute.  Since oai_dc is required, if that is "" or NULL all formats for that record will be considered deleted.

Required perl modules: POSIX, XML::LibXML, DBI, Encode


=head1 $op->new( hash_ref )

   { configFile => "oai_provider_conf.xml",
     logFile    => "oai_provider.log",
     url        => "http://some.url.org/OAI",
     arguments  => $args, ($args->{verb} = "ListSets")

     ## optional
     maxItems     => 500,                   ## default 100
     tableName    => "my_oai",              ## default "oai"
     setTableName => "my_oaisets",          ## default "oaisets"
     DBDriver     => "SQLite",              ## default "mysql"
     shortDate    => 1                      ## default 0, set to 1 if you want o use YYYY-MM-DD for granularity
     DbUpdate     => "2007-12-25 10:00:01", ## default to check update time of table
     aboutEnabled => 1,                     ## default 0, set to 1 if your data has <about> containers
                                            ## if this is set to 1, you NEED to have the about column in your DB
   }

When the database table holding the OAI data is altered (Update_time changes), any outstanding resumption tokens becomes invalid.  If your data is constantly updated, or the OAI table is frequently updated for some reason other than data changes, use the optional "DbUpdate" time in your CGI script.

=head1 $op->ConnectToDb( $db_user, $db_passwd, $db_name, $db_server )

returns 0 if failed to connect to DB

=head1 $op->BuildResponse()

Builds a response based on the cgi parameters passed in from new().  If a problem is found, an OAI error node is created and added to the response object

=head1 $op->toString();

Serialize the response.

=head1 $op->DisconnectDb();

The connection to the DB should be closed when the CGI script terminates.  This may be useful if your CGI script is doing something else after getting the response.

=head1 configuration 

An XML configuration file must be passed to the UMProvider in new().  This file contains the information for the Identify verb, ListSets verb, and all possible metadataFormats (for ListMetadataFormats). 

Sample config file:

 <?xml version="1.0" encoding="UTF-8"?>
 <oai_config>
   <Identify>
     <repositoryName>Your Repository</repositoryName>
     <baseURL>http://your.host.edu/OAI</baseURL>
     <protocolVersion>2.0</protocolVersion>
     <adminEmail>you@your.edu</adminEmail>
     <earliestDatestamp/>
     <deletedRecord>transient</deletedRecord>
     <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
     <description>
       <oai-identifier xmlns="http://www.openarchives.org/OAI/2.0/oai-identifier" 
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
           xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai-identifier           
           http://www.openarchives.org/OAI/2.0/oai-identifier.xsd">
         <scheme>oai</scheme>
         <repositoryIdentifier>your.host.edu</repositoryIdentifier>
         <delimiter>:</delimiter>
         <sampleIdentifier>oai:your.host.edu:000000001</sampleIdentifier>
       </oai-identifier>
     </description>
   </Identify>
   <ListSets>
     <set>
       <setSpec>foo</setSpec>
       <setName>All things of foo</setName>
     </set>
     <set>
       <setSpec>bar</setSpec>
       <setName>All things of bar</setName>
     </set>
   </ListSets>
   <PossibleMetadataFormats>
     <metadataFormat>
       <metadataPrefix>oai_dc</metadataPrefix>
       <schema>http://www.openarchives.org/OAI/2.0/oai_dc.xsd</schema>
       <metadataNamespace>http://www.openarchives.org/OAI/2.0/oai_dc</metadataNamespace>
     </metadataFormat>
     <metadataFormat>
       <metadataPrefix>marc21</metadataPrefix>
       <schema>http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd</schema>
       <metadataNamespace>http://www.loc.gov/MARC21/slim</metadataNamespace>
     </metadataFormat>
   </PossibleMetadataFormats>
 </oai_config>
  
=head1 KNOWN ISSUES

