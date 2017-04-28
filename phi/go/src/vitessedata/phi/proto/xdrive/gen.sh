protoc --go_out=plugins=grpc:. -I . xdrive_data.proto
protoc --python_out=../../../../../../py2/vitessedata/phi -I . xdrive_data.proto
