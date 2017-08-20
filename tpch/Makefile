gendata:
	go test -run=GenData ./bench
	@echo "NOTE: You still need to start xdrive or gpfdist manually.   Automate gpfdist is too fragile."

load:
	@echo "NOTE: Make sure you have started xdrive or gpfdist !!!" 
	go test -run=Setup ./bench
	go test -run=NONO -bench=Load -benchtime=0s ./bench

