#!/bin/sh
zkCli rmr /nabu
zkCli create /nabu 'enki zk chroot'
zkCli create /nabu/enki_le 'enki leader election'
zkCli create /nabu/policies 'enki dynamic throttle policy store'
