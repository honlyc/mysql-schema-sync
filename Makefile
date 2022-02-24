TARGET=$(shell basename $(PWD))

all:
	go build -o $(TARGET)