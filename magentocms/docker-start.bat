d:
cd d:\work\my_components\docker-magento2\
docker-compose -p %%1 -f docker-compose-maven.yml up -d
docker exec proj4_web_1 install-magento
docker exec proj4_web_1 install-sampledata
