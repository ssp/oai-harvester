<?
/******************************************************************************************
	 vlib_oai_extended.incl.php
	 Harvester fuer OAI-Schnittstellen Version 1.4 - 31.08.2006
         2006 by Andres Quast <a.quast@gmx.de>
         CC-Lizenz
*******************************************************************************************/


/** ***********************
*
* Funktionen fuer das Verarbeiten der Records. Die Funktionen sind auf GEO-LEO abgestimmt 
* Sollen sie benutzt werden, muessen sie nach den eigenen Beduerfnissen angepasst werden:
* 
* addServerSpecificInfos - verschiedene Informationen werden in den Records veraendert oder zugefuegt.
* Was jeweils gemacht wird, haengt von dem Repository ab, aus dem der Record stammt
* 
* testServerSpecificElements - wenn dc:source leer ist und der Record aus dem HAL-Repository kommt, 
* wird der Record verworfen, weil dahnintwer kein Volltext ist. Diese Funktion ist rudimentaer 
* und soll spaeter erweitert werden
* 
**************************/

function addServerSpecificInfos($zeile, $repName)
{
//Hinzufuegen und Veraendern von dc:Feldern in Records je nach Repository-Herkunft

/** Repository-Name wird dem Record mitgegeben */
/*Muster-Array fuer Abkuerzungen schaffen*/
/*$muster = array( "<identifier>oai:134.76.163.148",
                 "<identifier>oai:gfz-potsdam.de",
                 "<identifier>oai:www.earth-prints.org",
                 "<identifier>oai:dsr.nio.org",
                 "<identifier>oai:Birkbeck",
                 "<identifier>oai:archiv.UB.Uni-Marburg.de",
                 "<identifier>oai:authors.library.caltech.edu",
                 "<identifier>oai:www.era.lib.ed.ac.uk",
                 "<identifier>oai:dalea.du.se",
                 "<identifier>oai:deepblue.lib.umich.edu",
                 "<identifier>oai:OxfordEPrints.OAI2",
                 "<identifier>oai:eprints.gla.ac.uk",
                 "<identifier>oai:dspace.hrz.uni-dortmund.de",
                 "<identifier>oai:eprints.anu",
                 "<identifier>oai:bora",
                 "<identifier>oai:espace.lis",
                 "<identifier>oai:hal.",
                 "<dc:description>Peer Reviewed</dc:description>",
                );

$ersatz = array("<identifier>rep_id=1</identifier><identifier>oai:134.76.163.148",
                "<identifier>rep_id=2</identifier><identifier>oai:gfz-potsdam.de",
                "<identifier>rep_id=3</identifier><identifier>oai:www.earth-prints.org",
                "<identifier>rep_id=4</identifier><identifier>oai:dsr.nio.org",
                "<identifier>rep_id=5</identifier><identifier>oai:Birkbeck",
                "<identifier>rep_id=6</identifier><identifier>oai:archiv.UB.Uni-Marburg.de",
                "<identifier>rep_id=7</identifier><identifier>oai:authors.library.caltech.edu",
                "<identifier>rep_id=8</identifier><identifier>oai:www.era.lib.ed.ac.uk",
                "<identifier>rep_id=9</identifier><identifier>oai:dalea.du.se",
                "<identifier>rep_id=10</identifier><identifier>oai:deepblue.lib.umich.edu",
                "<identifier>rep_id=11</identifier><identifier>oai:OxfordEPrints.OAI2",
                "<identifier>rep_id=12</identifier><identifier>oai:eprints.gla.ac.uk",
                "<identifier>rep_id=13</identifier><identifier>oai:dspace.hrz.uni-dortmund.de",
                "<identifier>rep_id=14</identifier><identifier>oai:eprints.anu",
                "<identifier>rep_id=15</identifier><identifier>oai:bora",
                "<identifier>rep_id=16</identifier><identifier>oai:espace.lis",
                "<identifier>rep_id=17</identifier><identifier>oai:hal.",
                "<dc:type>Peer Reviewed</dc:type>",
                );

               for ($k=0; $k<count($muster); $k++)
                   {
                   $zeile = str_replace($muster[$k], $ersatz[$k], $zeile);
                   }
*/

$muster1 = '<header>';
$ersatz1 = "<header>\n<repname>".$repName."</repname>";
if(is_int($test = strpos($zeile, "<repname>")) == FALSE)
    {
    $zeile = str_replace($muster1, $ersatz1, $zeile);
    }


            //Earth-Prints bekommt Info ueber Zugangsrechte und dc:relations mit Zitaten werden entfernt
            if(is_int($test = strpos($zeile, "<dc:format>text/html</dc:format>")) && is_int($test2 = strpos($zeile, "<repname>Earth-Prints")))
                {
                if(is_int($test = strpos($zeile, "access restricted</dc:rights>")) == FALSE)
                    {
                    $zeile = str_replace("</oai_dc:dc>", "<dc:rights>access restricted</dc:rights></oai_dc:dc>", $zeile);
                    }
                //dc:relation bearbeiten:
                $zeile = rtrim($zeile);
                $recordArray = explode('</record>', $zeile);//mehrere Records voneinander trennen

                $token1 = '</dc:subject><dc:relation>';
                $token2 = '</dc:identifier><dc:relation>';
                $endToken = '</dc:relation><dc:contributor>';

                for($i=0; $i<count($recordArray); $i++)
                    {
                    if($recordArray[$i] != "")
                        {
                        if(is_int($test = strpos($zeile, $token1)))
                            {
                            $token = $token1;
                            $tReplacer = '</dc:subject><dc:contributor>';
                            }
                        else
                            {
                            $token = $token2;
                            $tReplacer = '</dc:identifier><dc:contributor>';
                            }

                        $elementArray = explode($endToken, $recordArray[($i)]);
                        $beforeElementArray = explode($token, $recordArray[($i)]);


                        $k = 0;
                        $zeile = $beforeElementArray[0].$tReplacer.$elementArray[(count($elementArray)-1)]."</record>";
                        }
                    }
                }

            //Glasgow bekommt Info ueber Zugangsrechte
            if((is_int($test = strpos($zeile, "<dc:format>")) == FALSE) && (is_int($test2 = strpos($zeile, "<repname>Glasgow"))) && (is_int($test2 = strpos($zeile, "<dc:rights>access")) == FALSE))
                {
                $zeile = str_replace("</oai_dc:dc>", "<dc:rights>access restricted</dc:rights></oai_dc:dc>", $zeile);
                }

            //Digizeitschriften bekommt korrigierte ISSN
            if(is_int($test = strpos($zeile, "<repname>DigiZeit")))
                {
                $zeile = str_replace("ISSN:", "", $zeile);
                }

            //DOAJ
            if(is_int($test = strpos($zeile, "<repname>DOAJ")))
                {
                $zeile = str_replace("oaidc:dc", "oai_dc:dc", $zeile);
                }

            //rero
            if(is_int($test = strpos($zeile, "<repname>Westschweizer")))
                {
                $zeile = str_replace("oaidc", "oai_dc", $zeile);
                }

            //scielos
            if(is_int($test = strpos($zeile, "<repname>Scielo")))
                {
                $zeile = str_replace("oai-dc", "oai_dc", $zeile);
                $zeile = str_replace("<![CDATA[", "", $zeile);
                $zeile = str_replace("]]>", "", $zeile);
                }

            //Austin Texas Karten bekommen Type Info
            if(is_int($test = strpos($zeile, "<setSpec>txdot")))
                {
                $zeile = str_replace("</oai_dc:dc>", "<dc:type>map</dc:type></oai_dc:dc>", $zeile);
                }

            //NERC: URL in dc:identifier schreiben
            if(is_int($test = strpos($zeile, "<repname>NERC")))
                {
                $zeile = str_replace("dc:relation", "dc:identifier", $zeile);
                }

            //bei Naturalis werden an dieser Stelle die entsprechenden Records aussortiert
            //zunächst hardcodiert :-(
            /*if(is_int($test = strpos($zeile, "<repname>Naturalis")))
                {
		$zeile = str_replace("oaidc", "oai_dc", $zeile);
                $limiter = "Scripta";
                $dcElement = 'dc:source';
                $zeile = selectRecords($dcElement, $limiter, $zeile);
                }*/

return($zeile);
}

function testServerSpecificElements($zeile)
{
$limitzeile = "";

//Aus Hal nur die Daten uebernehmen, die als Volltext vorliegen
if(is_int($test = strpos($zeile, "<dc:source>")) == FALSE && is_int($test2 = strpos($zeile, "<repname>Hal")))
    {
    $zeile = "";
    }

//Aus UCL nur die Daten uebernehmen, die als Volltext vorliegen
if(is_int($test = strpos($zeile, "<dc:format>")) == FALSE && is_int($test2 = strpos($zeile, "eprints.ucl.ac.uk")))
    {
    $zeile = "";
    }

//Aus Open University nur die Daten uebernehmen, die als Volltext vorliegen
if(is_int($test = strpos($zeile, "<dc:format>")) == FALSE && is_int($test2 = strpos($zeile, "open.ac.uk")))
    {
    $zeile = "";
    }

//Aus AWI nur die Daten uebernehmen, die als Volltext vorliegen und Artikel sind
if(is_int($test = strpos($zeile, "<dc:format>application/pdf")) == FALSE && is_int($test2 = strpos($zeile, "oai:awi.de")))
    {
    $zeile = "";
    }
if(is_int($test = strpos($zeile, "<dc:type>text, article")) == FALSE && is_int($test2 = strpos($zeile, "oai:awi.de")))
    {
    $zeile = "";
    }

//Aus Naturalis nur die Daten uebernehmen, die als zu Scripta Geologica gehoeren
if(is_int($test = strpos($zeile, "<dc:source>Scripta")) == FALSE && is_int($test2 = strpos($zeile, "oai:naturalis")))
    {
    $zeile = "";
    }

//Aus Soton nur die Daten uebernehmen, die als freier Volltext vorliegen, und Artikel sind.
if(is_int($test = strpos($zeile, "<dc:format>application/pdf")) == FALSE && is_int($test2 = strpos($zeile, "oai:eprints.soton")))
    {
    $zeile = "";
    }
if(is_int($test = strpos($zeile, "<dc:type>Article")) == FALSE && is_int($test2 = strpos($zeile, "oai:eprints.soton")))
    {
    $zeile = "";
    }
if(is_int($test = strpos($zeile, "<dc:relation>https://secure")) && is_int($test2 = strpos($zeile, "oai:eprints.soton")))
    {
    $zeile = "";
    }

//Aus Sussex nur die Daten uebernehmen, die als Volltext vorliegen
if(is_int($test = strpos($zeile, "<dc:format>application/pdf")) == FALSE && is_int($test2 = strpos($zeile, "sussex.ac.uk")))
    {
    $zeile = "";
    }

//Aus NERC nur die Daten uebernehmen, die als Volltext vorliegen
if(is_int($test = strpos($zeile, "<dc:format>application/pdf")) == FALSE && is_int($test2 = strpos($zeile, "http://nora.nerc")))
    {
    $zeile = "";
    }

$limitzeile = $zeile;


return($limitzeile);
}

?>