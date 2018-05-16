install: 
	go get -t ./bench

db:
	go test -run=NONO -bench=DB -benchtime=0s -timeout 24h ./bench

fdb: 
	go test -run=NONO -bench=Foundation -benchtime=0s -timeout 24h ./bench

