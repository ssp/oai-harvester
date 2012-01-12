<?
/***********************************************
*Klassenbibliothek Version 1.2 - 18.05.2006
*Andres Quast a.quast@gmx.de 
***********************************************/


class xmlwork{
/*Klasse zum Verarbeiten von XML-Dateien unter PHP4 und PHP5*/
//var...
var $sxe;                       //@object, Enthaelt den gesamten XML-File
var $url;                       //@string, Die abzufragende URL
var $resumptionUrl;             //@string, Die um den resumptionToken erweiterte URL
var $error;                     //@string, Ausgabe von Fehlern
var $version;                   //Versionsbestimmung
var $elementArray;              //Ergebnisarray fuer die Elemente
var $attributArray;             //Ergebnisarray fuer die Attribute
var $i_err = 0;                 //Ergebniszaehler
var $pathImport;     //@string, Pfad des Importverzeichnisses
var $file;                      //@string, Dateiname der entstehenden XML-Datei
var $fetchFile;                 //@string, Dateiname der Datei, die bei Parserfehlern angelegt wird 

var $verbose=0; 

function xmlwork($url, $file, $debug, $importDir)  //Konstruktor
{

$limiter = "";

$this->url = $url;
$this->file = $file;
$this->pathImport = $importDir;
$this->version = ereg("^4", phpversion());

    if($this->version) //Einlesen des XML-Files fuer PHP4
        {
        if(phpversion() == "4.3.4")
           {
                $this->error[$this->i_err] = "DOMxml: XML-Datei ".$this->url." konnte nicht korrekt eingelesen werden";
                $this->fetchFile = 'fetch'.$this->i_err++.'_'.$this->file;
                $fallback = $this->fetchXmlFile($this->pathImport, $this->fetchFile);
                unset($this->sxe);
           }
           
        elseif (!$this->sxe = @domxml_open_file($this->url))
           {
                $this->error[$this->i_err] = "DOMxml: XML-Datei ".$this->url." konnte nicht korrekt eingelesen werden";
                $this->fetchFile = 'fetch'.$this->i_err++.'_'.$this->file;
                $fallback = $this->fetchXmlFile($this->pathImport, $this->fetchFile);
           }
        }
    else //Einlesen des XML-Files fuer PHP5
        {

        if(!$this->sxe = @simplexml_load_file($this->url))
           {
                $this->error[$this->i_err++] = "SimpleXML: XML-Datei ".$this->url." konnte nicht korrekt eingelesen werden";
                $this->fetchFile = 'fetch'.$this->i_err++.'_'.$this->file;
                $fallback = $this->fetchXmlFile($this->pathImport, $this->fetchFile);
           }
        }
    
if($debug == 1)
    {
    $this->debugs();
    }

}//Konstruktor Ende


function debugs()//DEBUGGER
{

    $ergebnis = $this->elementArray;

    if($this->version && $this->sxe) //DEBUG Ausgabe des XML-Files fuer PHP4
        {
        $oaixml = ($this->sxe->dump_mem(true));

        $xmloutput = fopen ("debug.xml", 'a');
        fputs($xmloutput, $oaixml);
        fclose($xmloutput);
        unset($oaixml);
        unset($xmloutput);
        }

    elseif($this->sxe) //DEBUG Ausgabe des XML-Files fuer PHP5
        {
        $oaixml = $this->sxe->asXML();
        $xmloutput = fopen ("debug.xml", 'a');
        fputs($xmloutput, $oaixml);
        fclose($xmloutput);
        unset($oaixml);
        unset($xmloutput);
        }

return $ergebnis;
}//function Debug Ende


function resumptionGet() //Funktion stellt fest, ob ein resumptionToken existiert und erzeugt und behandelt in einem solchen Fall die n�hste OAI-Abfrage
{
$resumption = $this->element('resumptionToken');
if(!$resumption && !$this->sxe)
    {
    $resumption[0] = $this->fileTokenizer($this->pathImport, $this->fetchFile, 'resumptionToken');
    if($resumption[0] == "") unset($resumption);
    else
        {
        if($debug=1)
            {
            echo "ResumptionToken aus Datei extrahiert:\n".$resumption[0]."\n";
            } 
        }
    }

if($resumption)
    {
    $resumption[0] = trim($resumption[0]);
    if(($cutpos = strpos($this->url, '&from', 0)) != 0)
        {
        $baseUrl = substr($this->url,0,$cutpos);
        $this->resumptionUrl = $baseUrl.'&resumptionToken='.$resumption[0];
        }
    elseif(($cutpos = strpos($this->url, '&metadataPref', 0)) != 0)
        {
        $baseUrl = substr($this->url,0,$cutpos);
        $this->resumptionUrl = $baseUrl.'&resumptionToken='.$resumption[0];
        }
    elseif(($cutpos = strpos($this->url, '&resumptionToken', 0)) != 0)
        {
        $baseUrl = substr($this->url,0,$cutpos);
        $this->resumptionUrl = $baseUrl.'&resumptionToken='.$resumption[0];
        }
    else
        {
        $this->resumptionUrl = $this->url.'&resumptionToken='.$resumption[0];
        }
    }

return $this->resumptionUrl;
}//function resumptionGet Ende

function responseDateGet() //Function stellt das letzte ResponseDatum fest
{
$responseDate = $this->element('responseDate');
if(!$responseDate && !$this->sxe)
    {
    $responseDate = $this->fileTokenizer($this->pathImport, $this->fetchFile, 'responseDate');
    echo "ResponseDate aus Datei gelesen: ".$responseDate."\n";//DEBUG
    $lastDateTime = $responseDate;
    }
else
    {
    $lastDateTime = $responseDate[(count($responseDate)-1)];
    echo "ResponseDate aus DOM-Objekt gelesen: ".$responseDate."\n";//DEBUG
    foreach($responseDate as $temp)
        {
        echo "Array-Inhalt: ".$temp."\n";//DEBUG
        }
    }
            
$splitDate = explode('T', $lastDateTime);
$lastDate = trim($splitDate[0]);
echo "Letztes Datum: ".$lastDate."\n";
return($lastDate);
}//function responseDateGet Ende


/**
*Funktionen, fuer das zeilenweise XML-Parsen aus einer Datei
*
*fetchXmlFile - fragt URL oder Verzeichnis nach XML-Stream ab und kopiert diesen 
*in eine lokale Cache-Datei (fetch_Nr_Dateiname.xml).
*
*fileTokenizer - Liest eine Cache-Datei zeilenweise ein, sucht nach bestimmten Elementen *(Tags) und gibt den Inhalt des letzten Elements zurueck. 
*/

function fetchXmlFile($path, $file)
/**
*Fallback wenn Objekt-Modelle nicht funktionieren
*
*XML-Datei per URL /oder wenn schon existiert aus dem Verzeichnis/ abfragen und in *lokale Cache-Datei kopieren
*/

{
copy($this->url, $path.'/'.$file);//Ein XML-Stream oder eine bereits existierende Import-Datei wird in eine Cache-Datei geschrieben

//Nur wenn Datei aus dem Netz geladen wird, an bestehende Datei anhaengen
if(is_int($test = strpos($this->url, "http")) == TRUE)
    {
    //Tags korrigieren
    $this->trimTags();
    }
else
    {
    //removeFiles($this->url);
    //$this->trimTags();
    }
}//function fetchXmlFile Ende

function fileTokenizer ($path, $file, $token) //Funktion zum Zerlegen von xml-Dateien anhand von Tags
{
    if(!$fileHandle = fopen($path.'/'.$file, 'r')) 
        {
        $this->error[$this->i_err++] = "Datei = ".$path."/".$file." konnte nicht gelesen werden\n";
        echo "Datei = ".$path."/".$file." konnte nicht gelesen werden\n";
        } 
    else    
        {
        
        $i = 0;
        
        //$startpattern = '/<'.$token.'/';
        $startpattern = '/<'.$token.'.*?>/';
        $endpattern =  '/<\/'.$token.'>.*/';
        $cutpattern = '<'.$token.'>';
        $brokenpattern = '/<'.$token.'[^>]+/';
        while($rawzeile = fgets($fileHandle))
            {
            
            // Einzelne Zeilen mit Daten raussuchen
            $i = 0;
            //$z = 0;
        
    
            if(preg_match($startpattern, $rawzeile) && preg_match($endpattern, $rawzeile))
                {
                $zeile[$i++] = $rawzeile;
                //echo "startpattern getroffen\n";
                }

            if(preg_match($brokenpattern, $rawzeile))
                {
                //echo "Brokenpattern getroffen\n";

                $workzeile = trim($rawzeile);
                $openTag = true;
                }
            
            if($openTag)
                {
                $workzeile .= trim($rawzeile);
                if(is_int($test = strpos($rawzeile, ">")))
                    {
                    $openTag = false;
                    $zeile[$i++] = $workzeile;
                    //echo "Workzeile: ".$workzeile."\n";

                    }
                }

            }
        
        $k = 0;
        $i = count($zeile);
        if($this->verbose == 1 && isset($zeile))
            {
            foreach($zeile as $temp)
                {
                echo "Inhalt der Zeile ".$k++.': '.$temp."\n"; 
                }
            }
        //$cutzeile = strstr($zeile[($i-1)], $cutpattern);
        $cutzeile = preg_split($startpattern, $zeile[($i-1)]); //gibt das Startpattern und den Rest der Zeile zurueck
        $content = preg_replace($endpattern, "", $cutzeile[1]); //
        if($this->verbose == 1)
            {
            echo "Inhalt: ".$cutzeile[1]."\n";
            }

        fclose($fileHandle);
        }

        if($this->verbose == 1)
            {
            echo "Inhalt: ".$content."\n";
            }

return($content);
}//function fileTokenizer


function trimTags()
//Funktion entfernt in der Cache-Datei ueberfluessige und stoerende (Leer-)Zeichen aus den Tags
{
    Global $useDCParse;
    
    $zeile = "";
    if(!$fileHandle = fopen($this->pathImport.'/'.$this->fetchFile, 'r')) 
        {
        $this->error[$this->i_err++] = "Datei = ".$this->pathImport.'/'.$this->fetchFile." konnte nicht gelesen werden\n";
        echo "Datei = ".$this->pathImport.'/'.$this->fetchFile." konnte nicht gelesen werden\n";
        } 
    else    
        {
        while($rawzeile = fgets($fileHandle))
            {
            if($useDCParse)//Abfrage fuer IPort-Nutzer wie Marianna :-)
                {
                $rawzeile =  str_replace('<dc:', '<', $rawzeile);
                $rawzeile =  str_replace('</dc:', '</', $rawzeile);
                }
            
	    $zeile .= str_replace(' >', ">", $rawzeile);//Ueberfluessige Leerzeichen aus Tags entfernen
            $zeile = str_replace('</record>',"</record>\n", $zeile);//hinter jeden Record einen Umbruch schreiben 
            
            //Korrigierte Cache-Datei an die vorhandene Import-Datei anhaengen
            if($newFileHandle = fopen($this->pathImport.'/'.$this->file, 'a'))
                {
                $output = $zeile;
                fputs($newFileHandle, $output);
                fclose($newFileHandle);
                unset($zeile);
                }
            }
        fclose($fileHandle);
        }

}//function trimTags Ende

/**
Funktionen, fuer das XML-Parsen mit den Objekt-Modellen DOM und SimpleXML
*/

function writeXmlFile($path, $file)//XML-Datei aus der Variable sxe erzeugen oder Inhalte an bestehende Datei anhaengen
{
if(!$fileHandle = @fopen($path.'/'.$file, 'a'))
    {
    $this->error[$this->i_err++] = "Datei = ".$path."/".$file." konnte nicht angelegt werden<br>\n";
    } 
else
    {
    if($this->version && $this->sxe) //XML-File schreiben fuer PHP4
        {
        $oaixml = ($this->sxe->dump_mem(true));
        fputs($fileHandle, $oaixml);
        fclose($fileHandle);
        unset($oaixml);
        unset($fileHandle);
        }

    elseif($this->sxe) //XML-File schreiben fuer PHP5
        {
        $oaixml = $this->sxe->asXML();
        fputs($fileHandle, $oaixml);
        fclose($fileHandle);
        unset($oaixml);
        unset($fileHandle);
        }
    chmod($path.'/'.$file, 0777);
    }

}//function writeXmlFile Ende


function element($element)  //Element-Inhalte in einen Array lesen
{

    if($this->version && $this->sxe) //Elemente in Array lesen fuer PHP4
        {
        //if(($limiter != "") && ($this->sxe))
        if($limiter != "")
            {
            foreach ($this->sxe->get_elements_by_tagname($element) as $objekt_var)
                {
                foreach($objekt_var->attributes($element) as $temp)
                    {
                    if($temp->value == $limiter)
                        {
                        $this->elementArray[] = $objekt_var->get_content($element);
                        }
                    }
                 }
            }
        else
            {
            foreach ($this->sxe->get_elements_by_tagname($element) as $objekt_var)
                {
                $this->elementArray[] = $objekt_var->get_content($element);
                }
            }
        }
    else //Elemente in Array lesen fuer PHP5
        {
        if(($limiter != "") && ($this->sxe))
            {
            foreach($this->sxe->xpath("//$element") as $objekt_var)
                {
                foreach($objekt_var->attributes() as $temp)
                    {
                    if($temp == $limiter)
                        {
                        echo "Hier steht eine Debug-Variable: ".$objekt_var."<br>\n";
                        $this->elementArray[] = "$objekt_var";
                        }
                    }
               } 
            }
        
        elseif(($limiter == "") && ($this->sxe))
            {
            $temp =  $this->sxe->xpath("//$element");
            $this->error[$this->i_err++] = "Keine Zweite Variable angegeben";
            //$this->elementArray = '';
            
            
            foreach($this->sxe->xpath("//$element") as $objekt_var)
                {
                $temp = $objekt_var;
                $this->elementArray[] = $temp;
                }
            /*foreach ($sxe->children($element) as $objekt_var)
                {
                $temp =  $objekt_var->$inhalt;
                $this->elementArray[] = "$temp";
                }*/

            }
        elseif(!isset($this->sxe))
            {
            $this->error[$this->i_err++] = "Kein sxe-Objekt vorhanden"; 
            }
        else
            {
            $this->error[$this->i_err++] = "sxe-Objekt kann nicht ausgelesen werden";
            }
        }
        
return $this->elementArray;
}

function attribute($element, $attribute)  //Attribut-Inhalte eines Elementes in einen Array lesen
{

    if($this->version && $this->sxe) //Elemente in Array lesen fuer PHP4
        {
        foreach ($this->sxe->get_elements_by_tagname($element) as $objekt_var)
            {
            $this->elementArray[] = $objekt_var->get_attribute($attribute);
            }
        }
    
    else //Elemente in Array lesen fr PHP5
        {
        if(isset($attribute) && $this->sxe)
            {
            foreach($this->sxe->xpath("//$element") as $objekt_var)
                {
                $this->attributArray[] = "$objekt_var[$attribute]";
                }
            } 
        elseif(!isset($attribute) && $this->sxe)
            {
            $this->error[$this->i_err++] = '$attribute fehlt';
            }
        elseif(!isset($this->sxe))
            {
            $this->error[$this->i_err++] = "Kein sxe-Objekt vorhanden"; 
            }
        else
            {
            $this->error[$this->i_err++] = "sxe-Objekt kann nicht ausgelesen werden";
            }
        }
        
return $this->attributArray;
}

}//class xmlwork

?>