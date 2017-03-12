prefix ?= /usr/local

build:
ifndef GOPATH
	$(error GOPATH is undefined)
endif
	cd src/vitessedata/dgza && go get . && go install

install:
	install -d ${prefix}/dgza
	install scripts/* ${prefix}/dgza/
	install bin/dgza ${prefix}/dgza/
	install README.md ${prefix}/dgza/
