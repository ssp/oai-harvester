#!/usr/bin/php
<?php
/******************************************************************************************
	 oaidiggr.php
	 Harvester fuer OAI-Schnittstellen Version 1.4 - 31.08.2006
         2006 by Andres Quast <a.quast@gmx.de>
         CC-Lizenz
*******************************************************************************************/

/** ***********************
*
* Kommandozeilentool zum Harvesten von OAI_Schnittstellen.
* einzelne Records werden in ein fuer Zebra lesbares XML-Format geschrieben
* Version 1.4 (2006/08)
*
**************************/

/*Argumente aus der Standardeingabe ermitteln, Standardausgabe schreiben */
if (in_array($argv[1], array('--help', '-help', '-h', '-?')))
    {
    die("\n


Dieses Skript fragt OAI-Schnittstellen ab
und bereitet die empfangenen Daten fuer den Zebraserver auf.

Aufruf: ".$argv[0]." [optionen]

Optionen:
-h    Hilfe, zeigt diese Seite an. Mehr Hilfe in der Dokumentation
-v    Verbose, das Script erzeugt ein detailierte Ausgabe auf der Konsole
-v=2  Erweiterter Verbose-Level, besser nicht auf alten Rechnern probieren!
-d    Debug, Datei debug.xml wird geschrieben 
      (Achtung, verbraucht viel Plattenplatz)
-z    Zebraidx, Startet den Zebra-Indexer 
-Z    Starte nur den Zebra-Indexer / Kein Harvesting
-dc   Entferne dc: aus den Tags
-imp=[Verzeichnis] Verzeichnis in dass die importierten XML-Dateien eingelesen werden
-rec=[Verzeichnis] Verzeichnis in dass die Records geschrieben werden

Version 1.5 - 10/2006 
Andres Quast <a.quast@gmx.de>    

\n\n");
    }

$verbose = 0;
$debug = 0;
$zebraOn = FALSE;
$onlyZebra = FALSE;
$useParsExt = TRUE;
$useDCParse = FALSE;
$importDir = "";
$recDir = "";

for($i=1; $i<$argc; $i++)
    {
    if($argv[$i] == '-v') $verbose= 1;
    if($argv[$i] == '-v=2') $verbose= 2;
    if($argv[$i] == '-d') $debug= 1;
    if($argv[$i] == '-z') $zebraOn= TRUE;
    if($argv[$i] == '-Z') $onlyZebra = TRUE;
    if($argv[$i] == '-E') $useParsExt = FALSE;
    if($argv[$i] == '-dc') $useDCParse= TRUE;
    if(is_int($test = strpos($argv[$i], '-imp='))) $importDir = str_replace('-imp=' , '', $argv[$i]);
    if(is_int($test = strpos($argv[$i], '-rec='))) $recDir = str_replace('-rec=' , '', $argv[$i]);
    }

echo "Dieses PHP-Skript liest OAI-Schnittstellen.
Version 1.4 - 08/2006 
Andres Quast <a.quast@gmx.de>
\n";
echo "\n";
echo "PHP-Version: ".phpversion()."\n";
echo "Verbose-Level: ".$verbose."\n";
echo "Debug: ".$debug."\n";
if($useParsExt) echo "Nutze Parsing Extensions\n";
if($useDCParse) echo "Entferne \"dc:\" aus allen Tags\n";
if($zebraOn) echo "Starte anschliessend den Zebra-Indexer\n";
if($onlyZebra) echo "Starte nur den Zebra-Indexer\n";
if($importDir) echo "Nutze als Importverzeichnis: $importDir\n";
if($recDir) echo "Nutze als Recordverzeichnis: $recDir\n";

echo "\n";echo "\n";
/*Standardausgabe Ende */


/*Initialisieren von Konstanten und Variablen fuer die OAI-Abfrage*/
define('XMLDIR', 'import');   //xml-Ablageverzeichnis
define('RECDIR', 'records');   //xml-Ablageverzeichnis

if($importDir == "") $importDir = XMLDIR; 
if($recDir == "") $recDir = RECDIR; 

//Starte Skript
include('diggr_class.php');
include('diggr_harv.php');
include('diggr_ext.php');
include('diggr_zebra.php');
include('diggr_shell.php');


$repository = readTxt();

removeFiles($importDir.'/*fetch*');//Verlorene Cache-Dateien loeschen

if(!$onlyZebra) 
    {
    $repResults = harvesting($repository, $verbose, $useParsExt, $importDir, $recDir);
    }
else
    {
    $zebraOn = TRUE;
    }

zebraUpdate($zebraOn, $recDir);
renameFiles('.xml.txt .xml '.$importDir.'/*');//Cache-Dateien in Arbeitsdateien schreiben
removeFiles($importDir.'/*fetch*');//Verlorene Cache-Dateien loeschen
displayHits($repResults);

?>
