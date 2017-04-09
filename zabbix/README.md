DeepGreen Zabbix Agent
======================

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
Install golang from http://www.golang.org/ 
Check the Makefile.   Basically,
```
cd src/vitessedata/dgza && go get . && go install
install -d dest_dir
install scripts/* dest_dir
install bin/dgza dest_dir
install README.md dest_dir
```


Install Zabbix
--------------
User must have a Zabbix Server installed somewhere.  There are plenty of docs
on the web on how to install zabbix server.  https://www.zabbix.org/wiki contains
very detailed instructions.   We have a simple scritp to run a dockerized zabbix
server for dev and test, see https://github.com/vitesse-ftian/dockers/tree/master/zabbixserver

User also need to install zabbix_agent and zabbix_sender on DeepGreen master node.
Use CentOS as example,
```
# rpm -ivh http://repo.zabbix.com/zabbix/3.2/rhel/6/x86_64/zabbix-release-3.2-1.el6.noarch.rpm
# yum install --nogpgcheck -y zabbix zabbix-agent zabbix-sender 
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

Step 1-3 can be found at https://github.com/vitesse-ftian/dockers/tree/master/deepgreen.
Here is the scripts, suppose zabbix server is running at 172.20.0.6

```
# Configure, on DeepGreen master node.
sudo mkdir -p /usr/local/dgza
sudo sed -i 's/^Server=.*$/Server=172.20.0.6/g' /etc/zabbix/zabbix_agentd.conf
sudo sed -i 's/^ServerActive=.*$/ServerActive=172.20.0.6/g' /etc/zabbix/zabbix_agentd.conf
sudo sed -i 's/^Hostname=.*$/Hostname=dg/g' /etc/zabbix/zabbix_agentd.conf

# Build dgza, install into /usr/local/dgza, this is the default location.
# If user does not want to build on DeepGreen Master node, build it anywhere, then
# copy /usr/local/dgza dir to DeepGreen master and copy userparameter_dgza.conf to
# zabbix_agentd.d
cd dgtools/zabbix
make
sudo make install
sudo cp /usr/local/dgza/userparameter_dgza.conf /etc/zabbix/zabbix_agentd.d/
```


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

