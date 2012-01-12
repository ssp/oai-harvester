<?php

function zebraUpdate($zebraOn, $recDir)
{
if($zebraOn == TRUE)
    {
    echo "Zebra Index wird neu erstellt...\n";
    $command =  'cd /srv/www/htdocs/geoleo/oai_harvester/zebra; zebraidx -g oais update ../'.$recDir;
    $shell = shell_exec("$command");
    $command =  'cd zebra; zebraidx commit';
    $shell = shell_exec("$command");
    echo "Index ist fertig\n";
    }
}//function zebraUpdate Ende
 
?>