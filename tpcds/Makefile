gendata:
	go test -timeout 24h -run=GenData ./bench
	@echo "NOTE: You still need to start xdrive or gpfdist manually.   Automate gpfdist is too fragile."
	@echo "To start xdrive, do xdrctl start gen/xdrive.toml"
	@echo "To start gpfdist, you need to start gpfdist on each seghosts on port 22222."

ddl:
	go test -run=Setup ./bench

load:
	@echo "NOTE: Make sure you have started xdrive or gpfdist !!!" 
	go test -run=NONO -bench=Load -benchtime=1ns -timeout 24h ./bench

run:
	go test -run=NONO -bench=Query -benchtime=1ns -timeout 24h ./bench
