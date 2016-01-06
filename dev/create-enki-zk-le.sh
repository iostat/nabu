#!/bin/sh
zkCli rmr /nabu
zkCli create /nabu 'nabu zk chroot'
zkCli create /nabu/enki_le 'enki leader election'
