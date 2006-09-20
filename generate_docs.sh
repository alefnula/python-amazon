#!/bin/sh

rm -fr docs/s3/*
rm -fr docs/sqs/*
epydoc -o docs/s3 s3 --graph=all
epydoc -o docs/sqs sqs --graph=all
