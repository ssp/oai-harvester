<?
/******************************************************************************************
	 diggr_shell.php
	 Harvester fuer OAI-Schnittstellen Version 1.5 - 11.10.2006
         2006 by Andres Quast <a.quast@gmx.de>
         CC-Lizenz
*******************************************************************************************/

/**
diggr_shell.php enthaelt verschiedene Funktionen, die Shell-Kommandos
ausfuehren

*/
function removeFiles($path)
{
$command =  'rm '.$path;
$shell = shell_exec("$command");
echo "Raeume auf... ".$command."\n";
}//function removeFile Ende

function utfconditioner($path)
{
Global $verbose;

$command =  'cat '.$path.'| ./utf8conditioner -x > '.$path.'.bak';
$shell = shell_exec("$command");

$command =  'cp '.$path.'.bak '.$path;
$shell = shell_exec("$command");

if($verbose >= 1)
    {
    echo "UTF8 Korrektur mit... ".$command."\n";
    }

removeFiles($path.'.bak');

}//function utfconditioner Ende

function renameFiles($path)
{
$command =  'rename '.$path;
$shell = shell_exec("$command");
echo "Benenne Dateien um... ".$command."\n";
}//function renameFile Ende

function catFile($path)
{    
$command =  'cat '.$path.'/'.$this->fetchFile.' >> '.$path.'/'.$this->file;
$shell = shell_exec("$command");
echo "Cat-Befehl: ".$command."\n";
}//function catFile Ende

?> 
