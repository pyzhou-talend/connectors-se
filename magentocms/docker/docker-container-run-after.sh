#!/bin/bash
# parameters example:
# --docker-host 127.0.0.1 --magento-port 32867 --magento-port-secure 32868
# --magento-use-secure 0 --magento-use-secure-admin 0

# _setArgs(){
  # while [ "$1" != "" ]; do
    # case $1 in
      # "--docker-host")
        # shift
        # DOCKER_MACHINE_HOSTNAME=$1
        # ;;
      # "--magento-port")
        # shift
        # MAGENTO_PORT=$1
        # ;;
      # "--magento-port-secure")
        # shift
        # MAGENTO_PORT_SECURE=$1
        # ;;
      # "--magento-use-secure")
        # shift
        # MAGENTO_USE_SECURE=$1
        # ;;
      # "--magento-use-secure-admin")
        # shift
        # MAGENTO_USE_SECURE_ADMIN=$1
        # ;;
    # esac
    # shift
  # done
# }

# echo 'container setup started'
# DOCKER_MACHINE_HOSTNAME=127.0.0.1
# MAGENTO_PORT=80
# MAGENTO_PORT_SECURE=443
# MAGENTO_USE_SECURE=0
# MAGENTO_USE_SECURE_ADMIN=0

# _setArgs $*

# export MAGENTO_BASE_URL=http://$DOCKER_MACHINE_HOSTNAME:$MAGENTO_PORT
# export MAGENTO_BASE_URL_SECURE=https://$DOCKER_MACHINE_HOSTNAME:$MAGENTO_PORT_SECURE
# export MAGENTO_BASE_URL1=http://$DOCKER_MACHINE_HOSTNAME:$MAGENTO_PORT
# export MAGENTO_BASE_URL_SECURE1=https://$DOCKER_MACHINE_HOSTNAME:$MAGENTO_PORT_SECURE

###########################
echo start mysql
touch /var/run/mysqld/mysqld.sock
touch /var/run/mysqld/mysqld.pid
chown -R mysql:mysql /var/lib/mysql
chown -R mysql:mysql /var/run/mysqld
chown -R mysql:mysql /var/log/mysql
chmod -R 777 /var/run/mysqld/
/etc/init.d/mysql restart

echo set owner for www
#chmod -R 777 /var/www/html
#chown -R www-data:www-data /var/www/html

echo $MAGENTO_BASE_URL
echo $MAGENTO_BASE_URL_SECURE
echo $MAGENTO_USE_SECURE
echo $MAGENTO_USE_SECURE_ADMIN
set +x
su www-data <<EOSU

cd /var/www/html/bin/
./magento setup:store-config:set --base-url="$MAGENTO_BASE_URL"
./magento setup:store-config:set --base-url-secure="$MAGENTO_BASE_URL_SECURE"
./magento setup:store-config:set --use-secure="$MAGENTO_USE_SECURE"
./magento setup:store-config:set --use-secure-admin="$MAGENTO_USE_SECURE_ADMIN"
./magento cache:flush

EOSU
set -x
echo 'container setup finished'

echo start apache
echo ServerName www.example.com >> /etc/apache2/apache2.conf
source /etc/apache2/envvars && exec /usr/sbin/apache2 -DFOREGROUND

###########################

