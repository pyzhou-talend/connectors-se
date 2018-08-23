https://cloudkul.com/blog/magento-2-docker-installation/

do this after container is started ()
$ docker exec -it 088b6f4849b4 /var/www/html/bin/magento setup:store-config:set --base-url="http://192.168.99.100:32805/"
$ docker exec -it 088b6f4849b4 /var/www/html/bin/magento cache:flush