PTNK mysql storage engine
=========================

## **WARNING**
Ptnk storage engine is still at very early stage of development.

**DO NOT STORE VALUABLE DATA YET**

## TODO
* update of indexed columns
* update/delete by secondary keys

## How to try
* apt-get install cmake
* get mysql source dist
* configure/build using debug settings
  * need to remove -Werror from cmake/maintainer.cmake to build with latest gcc?
* BUILD/compile-amd64-debug-max-no-ndb
* install to default path: /usr/local
`sudo make install`
* make mysql workdir
`mkdir -p ~/work/myvar`
* link to source pkg
`sudo ln -s ~/data/Downloads/mysql-5.6.2-m5 /usr/src/mysql`
* make plugin dir (/usr/local/mysql/lib/plugin doesn't work)
```
sudo mkdir -p /usr/local/mysql/share/lib/plugin
sudo chmod 777 /usr/local/mysql/share/lib/plugin
```
* build my_ptnk
`../waf configure build install`
* init mysql
`./mysqld_clean`
* debug run mysql
`./mysqld_debugrun`
* load myptnk plugin
`install plugin myptnk soname 'ha_myptnk.so';`
