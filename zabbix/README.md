DeepGreen Zabbix Agent
======================

User must have a Zabbix Server installed somewhere.  On DeepGreen master node user
must have installed zabbix\_agent and zabbix\_sender.   There are several docker
files in vitesse-ftian/dockers reposity that will set up a working dev environment.

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




