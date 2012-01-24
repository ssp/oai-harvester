<?php
/******************************************************************************************
	 diggr_harv.php
	 Harvester fuer OAI-Schnittstellen Version 1.4 - 31.08.2006
         2006 by Andres Quast <a.quast@gmx.de>
         CC-Lizenz
*******************************************************************************************/


/** ***********************
*
* Kernfunktionen fuer das Harvesten von OAI-MPH Schnittstellen:
* 
* readText - liest alle Repository-Daten aus der repositories.txt
* in das Array @repository 
*
* harvesting - ermittelt den letzen Harvestingstand und kontrolliert den Harvesting-Prozess 
*
* xmlTokenizer - zerlegt die geharvesteten XML-Dateien in einzelne Records und startet verschiedene
* Weiterverarbeitungsroutinen. Schreibt die Records danach einzeln oder gebuendelt in ein definiertes
* Verzeichnis
* 
* selectRecords - ermittelt, ob ein Record ein gesuchtes dc:subject Feld enthaelt oder nicht
*
* extractElementfromFile - waehlt aus einem wiederholbaren dc:feld das n-te Feld aus und verwirft 
* die anderen bevor der record geschrieben wird
*
* removeFiles - unoetige und stoerende Dateien werden entfernt
*
* displayErrors - schreibt im Verbose-Modus Fehler in die Standardausgabe
*
* displayHits - schreibt die Anzahl der Treffer pro Repository und Set in die Standardausgabe
* 
**************************/

/*Einlesen der Repositories und ihrer Abfragemodalitäten aus der Datei repositories.txt*/
function readTxt() {
	$repositories = Array();

	$oai_datei = fopen('repositories.txt', "r");
	while($rawzeile = fgets($oai_datei)) {
		//Zeile ab '#' ignorieren
		if(($cutpos = strpos($rawzeile,'#')) !== FALSE) {
            $rawzeile = substr($rawzeile,0,$cutpos);
		}

		$zeile = explode(",", $rawzeile);
		if(count($zeile) > 2) {
			$repositories[] = Array(
				// Bezeichnung des Repositorys
				'Name' => trim($zeile[0]),
				// Abzufragende URL
				'Url' => trim($zeile[1]),
				// Dateiname der späteren XML-Datei
				'File' => trim($zeile[2]),
				// Art der OAI-Antwort
				'Prefix' => (count($zeile) > 3) ? trim($zeile[3]) : '',
				// Abzufragendes Set
				'Set' => (count($zeile) > 4) ? trim($zeile[4]) : '',
				// Filter für geowissenschaftliche Arbeiten
				'Select' => (count($zeile) > 5) ? trim($zeile[5]) : ''
			);
		}
	}

	return($repositories);
}



/**Einlesen der OAI-PMH Abfrage in die entsprechende XML-Datei*/
function harvesting ($repositories, $verbose, $useParsExt, $importDir, $recDir) {
	global $debug;
	foreach ($repositories as $repository) {
		$queryURL = $repository['Url'] . 'verb=ListRecords&metadataPrefix=' . $repository['Prefix'];

		if($repository['Set'] != "") {
	        $queryURL .= '&set='.$repository['Set'];
        }

		if(!is_file($importDir.'/'.$repository['File'])) {
			/**
			 * Fall 1: Import-Datei existiert noch nicht, also
			 * erste Abfrage eines Repositories
			 */
			echo "XML-Datei »" . $repository['File'] . "« existiert noch nicht: Harveste Repository »" . $repository['Name'] . "« und erzeuge neue XML-Datei.\n";

			$OAIQuest = new xmlwork($queryURL, $repository['File'], $debug, $importDir);

			$writeFile = $OAIQuest->writeXmlFile($importDir, $repository['File']);
			$moreRecords = $OAIQuest->resumptionGet();
			$error = $OAIQuest->error;
			displayErrors($error);

			$k= 0;
			$resumptionURLs = Array();
			while($moreRecords != '') {
				echo "Harveste Repository: »" . $repository['Name'] . " Resumption-Nr.: " .$k . "\n";
				$OAIQuest = new xmlwork($moreRecords, $repository['File'], $debug, $importDir);

				$writeFile = $OAIQuest->writeXmlFile($importDir, $repository['File']);
				$moreRecords = $OAIQuest->resumptionGet();
				$error = $OAIQuest->error;
				displayErrors($error);

				$resumptionURLs[$k] = $moreRecords;

				// Prüfen, ob OAI-PMH Server keinen Fehler meldet
				if ($resumptionURLs[$k] == $resumptionURLs[$k-1]) {
					break;
				}

				if ($verbose == 1) {
					echo "Resumption-URL: " . $resumptionURLs[$k] . "\n";
				}
				$k++;
			}

			$today = date("Y-m-d");
			writeRootElement($repository, $importDir, $repository['File'], 'start', $today);

			$repository['Treffer'] = xmlTokenizer($importDir, $repository['File'], $recDir, $repository['File'], 'record', $repository['Select'], $verbose, $useParsExt, $repository['Name']);
			echo "Abfrage von »" . $repository['Name'] . "« beendet.\n\n";
		}
		else {
			/**
			 * Fall 2: Import-Datei existiert bereits
			 * also Prüfen und ggf. Abfragen eines Repositories
			 */
			echo 'XML-Datei »' . $repository['File'] . "« existiert bereits, prüfe, ob neue Einträge im Repository vorhanden sind\n";

			$xmlOrigFile = $importDir . '/' . $repository['File'];
			$queryURL = $repository['Url'] . 'verb=ListRecords&metadataPrefix=' . $repository['Prefix'];

			$getLastQuest = new xmlwork($xmlOrigFile, $repository['File'], $debug, $importDir);
			$lastDate = $getLastQuest->responseDateGet();
			$error = $getLastQuest->error;
			displayErrors($error);

			$today = date("Y-m-d");

			echo "Heutiges Datum: " . $today . "\n";
			echo "Letztes Harvesting Datum: " . $lastDate . "\n";
			$fromTimeSt = strtotime("$lastDate + 1 Day");
			//echo "Unix Timestamp: ".$fromTimeSt."\n";
			$fromDate =  date("Y-m-d", $fromTimeSt);

			if($today != $lastDate && $lastDate != "") {
				echo "Datum für das Harvesten: " . $fromDate . "\n";
				$queryURL = $repository['Url'] . 'verb=ListRecords&from=' . $fromDate . '&until=' . $today . '&metadataPrefix=' . $repository['Prefix'];

				if($repository['Set'] != "") {
					$queryURL .= '&set=' . $repository['Set'];
				}

				if($verbose >= 1) {
					echo "Abfrage-URL: " . $queryURL . "\n";
				}

				$OAINewQuest = new xmlwork($queryURL, $repository['File'], $debug, $importDir, $importDir);
				$writeFile = $OAINewQuest->writeXmlFile($importDir, $repository['File']);
				$moreRecords = $OAINewQuest->resumptionGet();
				$error = $OAINewQuest->error;
				displayErrors($error);

				$k= 0;
				$resumptionURLs = Array();
				while($moreRecords != "") {
					$OAINewQuest = new xmlwork($moreRecords, $repository['File'], $debug, $importDir);

					$writeFile = $OAINewQuest->writeXmlFile($importDir, $repository['File']);
					$moreRecords = $OAINewQuest->resumptionGet();
					$error = $OAINewQuest->error;
					displayErrors($error);

					$resumptionURLs[$k] = $moreRecords;

					// Prüfen, ob OAI-PMH Server keinen Fehler meldet
					if ($resumptionURLs[$k] == $resumptionURLs[$k-1]) {
						break;
					}

					if ($verbose == 1) {
						echo "Resumption-URL: " . $resumptionURLs[$k-1] . "\n";
					}
					$k++;
				}
			}
			else {
				echo "Der " . $lastDate . " ist heute, deshalb kein neues Harvesting.\n";
			}

			removeFiles($recDir . '/*' . $repository['File']);
			removeFiles($importDir . '/' . $repository['File'] . '.txt');

			writeRootElement($repository, $importDir, $repository['File'], 'start', $today);
			$repository['Treffer'] = xmlTokenizer($importDir, $repository['File'], $recDir, $repository['File'], 'record', $repository['Select'], $verbose, $useParsExt, $repository['Name']);

			echo "Daten sind aus »" . $repository['File'] . " extrahiert.\n\n";
		}

		writeRootElement($repository, $importDir, $repository['File'], 'stop', $today);
	}

	//removeFiles($importDir.'/*fetch*');
	return($repository);
}



//Funktion zum Zerlegen von xml-Dateien anhand von Tags
function xmlTokenizer ($path, $file, $newpath, $newfile, $token, $limiter, $verbose, $useParsExt, $repName) {
	utfconditioner($path.'/'.$file);
	$newRow = '';
	if($fileHandle = fopen($path.'/'.$file, 'r')) {
    unset($zeile);
    echo "Extrahiere Daten aus ".$file.".\n";
    $zahl = 20;
    $k = $zahl;
	$i = 0;
    $j = 0;
    $cum = 1;
	$mehrZeilen = Null;
	
    while($rawzeile = fgets($fileHandle))
        {
        if ($verbose == 2)
            {
            echo "Zeile ".($i+1).".\n";
            echo $rawzeile;
            }

        // Einzelne Zeilen mit Daten raussuchen
        $i++;
        $z = 0;

        $startpattern = '/<'.$token.'>/';
        $endpattern =  '/<\/'.$token.'>/';

        if(preg_match($startpattern, $rawzeile) && preg_match($endpattern, $rawzeile))
        // 1. Fall Start und Endmuster in einer Zeile
            {
            $zeile = strstr ($rawzeile, "<".$token.">");
            $mehrZeilen = FALSE;
            }

        if(preg_match($startpattern, $rawzeile) && !preg_match($endpattern, $rawzeile))
        // 2. Fall Startmuster ohne Endmuster in Zeile
            {
            $zeile = strstr ($rawzeile, "<".$token.">");
            $mehrZeilen = TRUE;
            }

        if($mehrZeilen == TRUE && !preg_match($startpattern, $rawzeile) && !preg_match($endpattern, $rawzeile))
        //Zeilen zwischen Start und Endmuster rauschreiben und anhaengen
            {
            $zeile .= $rawzeile;
            }

        if($mehrZeilen == TRUE && preg_match($endpattern, $rawzeile))
        //Zeile mit Endmuster anhaengen, $mehrZeilen auf false setzen, damit Zeile in Datei geschrieben wird
            {
            $zeile .= $rawzeile;
            $mehrZeilen = FALSE;
            }

        if($mehrZeilen == FALSE && isset($zeile))//Nachbearbeitung und anschliessendes Schreiben der Zeile in Datei
            {
            $l = $j/$k;
            if($l == 1)//Pruefen, ob neue Record-Datei angelegt werden soll, oder Zeile an bestehende Datei angehaengt wird
                {
                $newfile = ereg_replace('^[0-9]*',"",$newfile);
                $newfile = ($k/$zahl).$newfile;
                $k = $k+$zahl;
                }
            if($limiter != "")
                {
                $dcElement = 'dc:subject';
                $zeile = selectRecords($dcElement, $limiter, $zeile);
                }
            if($zeile != "")
                {
                $zeile = testServerSpecificElements($zeile);
                }
            if($zeile != "")
                {

                $j++;
                $zeile = addServerSpecificInfos($zeile, $repName);
                if($useParsExt)
                    {
                    $zeile = extractElementfromFile('<dc:date>', $zeile, 'last');
                    }

//                $treffer++;

                //neuschreiben der import-datei
                if($j <= (100*$cum))
                    {
                    $newRow .= $zeile."\n";
                    }
                else
                    {
                    $newRow .= $zeile."\n";
                    if($bakFileHandle = fopen($path.'/'.$file.'.txt', 'a'))
                        {
                        writeContentXml($newRow, $path, $file);
                        /*$output = $newRow;
                        fputs($bakFileHandle, $output);
                        fclose($bakFileHandle);*/
                        $cum++;
                        unset($newRow);
                        }
                    }

                //recorddatei schreiben
                if($newFileHandle = fopen($newpath.'/'.$newfile, 'a'))
                    {
                    //echo "Schreibe Zeile: ".$zeile;
                    $output = $zeile."\n";
                    fputs($newFileHandle, $output);
                    fclose($newFileHandle);
                    unset($zeile);
                    }

                }
            }
        }
        fclose($fileHandle);

    }
    $treffer = $j;
    echo "Anzahl Treffer: ".$treffer."\n";
    writeContentXml($newRow, $path, $file);
    return($treffer);
}


function selectRecords($dcElement, $limiter, $zeile)
/** Diese Funktion selektiert Records anhand von dc:subject-Elementen und gibt 
nur die gesuchten zurueck */
{
Global $verbose;

$keywords = explode(';', $limiter);
$limitzeile = "";


foreach($keywords as $temp)
    {
    //echo "Begrenzer: ".$temp."\n";
    $limitkey = '/<'.$dcElement.'.?'.$temp.'/i';
    //$limitkey = $temp;
    if(preg_match($limitkey, $zeile))
        {
        $limitzeile = $zeile;
        if($verbose ==2)
            {
            echo "Limitzeile: ".$limitzeile."\n---\n";
            }
        }
    }
return($limitzeile);
}

function extractElementfromFile($token, $zeile, $position)
/** 
*Diese Funktion sucht einen Element-Inhalt raus, 
*verwirft die anderen und
*schreibt nur diesen zurueck in den Record-File. 
*/

{
Global $verbose;

$ergebnisZeile = "";
$endToken = str_replace('<', "</", $token);

/** 
*Schritt 1: Alle in einer Datei enthaltenen Records werden voneinander getrennt 
*und in das recordArray geschrieben.
*/

$zeile = rtrim($zeile);
$recordArray = explode('</record>', $zeile);


/** Schritt 2: Jeder Record in recordArray wird an den Endtoken voneinander getrennt und in das 
    und in das elementArray geschrieben.
    Da der erste Teil des Records, vor dem ersten Token noch gebraucht wird, wird er in ein
    extra beforeElementArray geschrieben. 
    */
for($i=0; $i<count($recordArray); $i++)
    {
    if($recordArray[$i] != "")
    {
    $elementArray = explode($endToken, $recordArray[($i)]);
    $beforeElementArray = explode($token, $recordArray[($i)]);

/** Schritt 3: Die Variable position gibt an, welcher Token-Inhalt (Elementinhalt)in den Record 
    zurueckgeschrieben werden soll, und welche Token-Inhalte aus dem Record entfernt werden.
    Da man nicht wissen kann, wieviele Token-Inhalte in einem Record enthalten sind, 
    muss das skript auch relative Angaben (first, last) verstehen und korrekt umsetzen. Die
    Gesamtzahl der Tokenelemente ist die Anzahl der elementArrays -2 da das Letzte 
    Arrayfeld keinen Tokeninhalt enthält und das erste Feld mit 0 indexiert ist.  
    */
 
    if($position == 'first')
        {
        $positionZahl = 1;
        }

    if($position == 'last')
        {
        $positionZahl = count($elementArray)-2;
        }
    if($verbose == 2)
        {
        //echo 'Zeile: '.$recordArray[($i)]."\n";
        echo 'Counter: '.count($elementArray)."\n";
        echo 'Position: '.$positionZahl."\n";
        //echo 'Element: '.$elementArray[$positionZahl]."\n";
        //echo 'Element -1: '.$elementArray[($positionZahl-1)]."\n";
        }


/** Schritt 4: Je nach Inhalt der Variable positionZahl wird der Rekord 
    in unterschiedlicher Weise zusammengebaut:
    0 =   es gibt genau einen Tokeninhalt - der Record wird wieder im urspruenglichen Zustand
          zusammengesetzt
    -1 =  es gibt keinen Tokeninhalt - der Record wird wieder im urspruenglichen Zustand
          zusammengesetzt
    1-n = Der entsprechende Inhalt des n'ten Elements wird ausgelesen und in den Record geschrieben.
    */

    // Erster Fall: es ist kein oder ein Token enthalten:
    if(($positionZahl <= 0) && ($position != 'first'))
        {
        if($positionZahl == -1)
            {
            $ergebnisZeile .= $elementArray[0]."</record>";
            }
        elseif($positionZahl == 0)
            {
            $ergebnisZeile .= $elementArray[0].$endToken.$elementArray[1]."</record>";
            }
        }

     // Zweiter Fall: der abgefragte Token ist nicht der erste und vorhanden:
     elseif(($positionZahl > 0) && ($position != 'first'))
        {
            $ergebnisZeile .= $beforeElementArray[0].$elementArray[$positionZahl].$endToken.$elementArray[(count($elementArray)-1)]."</record>";
         }

    //Dritter Fall: Der erste Token ist vorhanden und wird abgefragt:
    elseif(($position == 'first') && (count($elementArray) > 0))
        {
        $k = 0;
        $ergebnisZeile .= $elementArray[0].$endToken.$elementArray[(count($elementArray)-1)]."</record>";
        echo "\n\n".'Debug: '.$ergebnisZeile."\n";
        foreach($elementArray as $temp)
            {
            echo 'Teil '.$k++.': '.$temp."\n";
            }
        //$ergebnisZeile .= $elementArray[0].$token.$elementArray[$position].$elementArray[(count($elementArray)-1)];
        }
    }
    }

return($ergebnisZeile);
}


function displayErrors($error) {
	global $verbose;

	if($error && $verbose >= 1) {
		foreach($error as $temp) {
			echo "! Fehler: " . $temp . "\n";
		}
	}
}



function displayHits($repositories) {
	if(isset($repositories)) {
		$gesamtTreffer = 0;
	
		foreach($repositories as $repository) {
			echo $repository['Treffer'] . " Treffer aus " . $repository['Name'] . " Set: " . $repository['Set'] . " geharvestet.\n";
	        $gesamtTreffer += $repository['Treffer'];
		}
		echo "\n\nSumme aller Treffer: " . $gesamtTreffer . "\n";
    }
}



function writeRootElement($repository, $path, $file, $part, $today) {
	$startstr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
    <OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"
    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
    xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/
    http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n".
'<responseDate>' . $today . "</responseDate>\n
<request verb=\"ListRecords\" metadataPrefix=\"oai_dc\" set=\"" . $repository['Set'] . '" resumptionToken="">' . $repository['Url'] . "</request>\n
  <ListRecords>";

	$stopstr = '<resumptionToken/></ListRecords></OAI-PMH>';
    
    if($part == 'start') {
        $output = $startstr;
    }
    else {
        $output = $stopstr;
    }
    
    if($bakFileHandle = fopen($path.'/'.$file.'.txt', 'a')) {
        fputs($bakFileHandle, $output);
        fclose($bakFileHandle);
    }
}



function writeContentXml($newRow, $path, $file) {
if($bakFileHandle = fopen($path.'/'.$file.'.txt', 'a'))
    {
    $output = $newRow;
    fputs($bakFileHandle, $output);
    fclose($bakFileHandle);
    }
}
?>
