protoc --go_out=plugins=grpc:../go/src/vitessedata/phi/proto/xdrive -I . xdrive_data.proto 
protoc --go_out=plugins=grpc:../../plugin/go/src/vitessedata/proto/xdrive -I . xdrive_data.proto 
protoc --python_out=../py2/vitessedata/phi -I . xdrive_data.proto
