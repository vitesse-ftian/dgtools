gendata:
	go test -timeout 24h -run=GenData ./bench
	@echo "NOTE: You still need to start xdrive or gpfdist manually.   Automate gpfdist is too fragile."
	@echo "For xdrive, just xdrctl start gen/xdrive.toml"
	@echo "For gpfdist, you need to start gpfdist on each seg hosts on port 22222"

ddl:
	go test -run=Setup ./bench

load:
	@echo "NOTE: Make sure you have started xdrive or gpfdist !!!" 
	go test -run=NONO -bench=LoadData -benchtime=1ns -timeout 24h ./bench

run:
	go test -run=NONO -bench=QueryDb -benchtime=1ns -timeout 24h ./bench

loadspq:
	@echo "NOTE: Make sure you have started xdrive or gpfdist !!!" 
	go test -run=NONO -bench=LoadSpq -benchtime=1ns -timeout 24h ./bench

runspq:
	go test -run=NONO -bench=QuerySpq -benchtime=1ns -timeout 24h ./bench

