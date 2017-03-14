# How To Add A Metric

## templates zbx_deepgreen_templates.xml
Import scripts/zbx_deepgreen_templates.xml into a Zabbix web UI.
Use the activity application as example, create new application,
item, trigger, etc.   Save, export.  

## userparameter_dgza.conf
If the templates defines a new application, you need to hook it up
in userparameter_dgza.conf.   If you did not add application, no need
to touch this file.   All Items are implemented in dgza golang code.

## golang code.
If you added application, hook it up in dgza.go, otherwise, just need
to change dgza/TheApplication.go.   TheApplication.go should be very 
simple.  All boilerplate code except the sql that it executes.   

Name of the items are defined in the zabbix templates.  golang application
should report it back to zabbix server simply use
```
    fmt.Printf("- deepgreen.name_of_item value\n")
``` 

## Example
git log 0a65fc42

## Random notes
* Do not print debug or help, etc, in golang to stdout.
* Do not fire application too frequently.  How frequently each application runs is defined in templates.  
* Do not run extremely complex sql -- monitoring should not tax the server 
too heavily.
* Do not write to database (or filesystem if you use deepgreen execute).  



