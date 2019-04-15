#!/bin/sh

vagrant ssh -- sudo journalctl -xef -u $1
