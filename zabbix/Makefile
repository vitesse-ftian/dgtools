prefix ?= ${TOOLCHAIN_DIR}/installed

build:
ifndef GOPATH
	$(error GOPATH is undefined)
endif
ifndef TOOLCHAIN_DIR
	$(error TOOLCHAIN_DIR is undefined)
endif
	cd src/vitessedata/dgza && go get . && go install

install:
	install -d ${prefix}/dgza
	install scripts/* ${prefix}/dgza/
	install bin/dgza ${prefix}/dgza/
	install README.md ${prefix}/dgza/


