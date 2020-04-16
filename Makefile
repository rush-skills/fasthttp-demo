.PHONY: api blaster

all: api blaster

api: api/main.go
	go build api/main.go
	mv main bin/api

blaster: blaster/main.go
	go build blaster/main.go
	mv main bin/blaster

