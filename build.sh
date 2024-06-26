#!/bin/bash

cd ucx
./autogen.sh && ./contrib/configure-release --prefix=$(pwd)/build/ && make -j && make install
cd ../
cd ucc
git checkout ibv_mcast_ag_test
./autogen.sh && ./configure --prefix=$(pwd)/build/ --with-ucx=$(pwd)/../ucx/build/ --with-tls=all && make -j && make install
cd ../
cd ompi
git submodule update --init --recursive
./autogen.pl && ./configure --prefix=$(pwd)/build/ --enable-prte-prefix-by-default --with-ucx=$(pwd)/../ucx/build/ --with-ucc=$(pwd)/../ucc/build/ && make -j && make install
cd ../
