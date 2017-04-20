# XDrive plugin

# Build
cd to src/vitessedata/fsplugin_csv, do make, or go install.   It should build a binary called fsplugin_csv
inside the bin dir.

# XDrive Toml 
In the example dir, make start will start an xdrive service.  Note that xdrive is started according to the
toml file, in the example, we just started one xdrive process.   Edit host in the toml file to start a 
cluster. 

The toml file defines a list of mount point, in the example, we have to mount points, one is called fs,
using builtin scheme nfs, the other is myx, using a plugin scheme called fsplugin.

The makefile make start will copy fsplugin_csv to the xdrplugin directory of the xdrive dir in the toml file.
If starting a cluster, the plugin must be copied to ALL hosts.

# SQL ddl.
Example are in xddl.sql, note the xdrive url syntax, 
        xdrive://host:port/mountpoint/path
Format can be any format that the external table ddl can accept.  In practice, one can use 'csv' as a catchall
format.  

# Plugin Implementation
SQL using external table will cause xdrive to invoke the plugin.   In our example, it will invoke fsplugin_csv.
In general, it will invoke xxx_yyy, where xxx is the scheme defined in the mount point in toml, yyy is the format.

The plugin must implement 4 methods, read, sample, size_meta, and write.  Our implementation is golang, but any
language that can serialize/deserialize protobuf should work.
