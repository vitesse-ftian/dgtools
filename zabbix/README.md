DeepGreen Zabbix Agent
======================

User must have a Zabbix Server installed somewhere.  On DeepGreen master node user
must have installed zabbix\_agent and zabbix\_sender.   There are several docker
files in vitesse-ftian/dockers reposity that will set up a working dev environment.

Design
------
Zabbix can (and should) monitor the OS level events on master and all segments.  
This agent should be installed/enabled on Master, to monitor the database related
activities/events.  While we can implement a segment agent running on each segement
and get database events using utility mode, we choose not to do so.  Instead we only
implement an agent on master and use management views, for example, those in gp\_toolkit
to read information.  This way, we can get better correlation of the state of the 
whole system.   

Build
-----
Check the Makefile.   Basically,
```
cd src/vitessedata/dgza && go get . && go install
install -d dest_dir
install scripts/* dest_dir
install bin/dgza dest_dir
install README.md dest_dir
```

To install DeepGreen Zabbix Agent 
---------------------------------

1. Edit dgza.sh, set correct deepgreen database connection info.  
   The deepgreen user should be a superuser and it should be able
   to login without a password.  
2. Install dgza and dgza.sh to a directory which zabbix agent has 
   exec permisson.  dgza and dgza.sh must resides in same dir.  
   By default we use /usr/local/dgza
3. Edit userparameter\_dgza.conf, pointing to dgza.sh and zabbix\_agentd.conf
4. Restart zabbix-agent

The docker file has done 1-3, but user need to start agent. 

To Enable Monitoring On Zabbix Server
-------------------------------------

1. Import deepgreen\_template.xml
2. Add the template to the Host.   

Step 2 need some browsing and clicking -- check Zabbix doc.

To Add/Change Zabbix Items
--------------------------
First, import the zbx\_deepgreen\_templates.xml into Zabbix web UI.  Make proper changes
to the template, then export.

If need to add a new application, change userparameter\_dgza.conf

Change golang code to implement the application and item.   Hopefully all you need is
to write SQL.

