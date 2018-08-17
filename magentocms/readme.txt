
list of all categories with hierarchy
local.magento/index.php/rest/V1/categories

list of products selected by search criteria
local.magento/index.php/rest/V1/products?searchCriteria[filter_groups][0][filters][0][field]=sku&
searchCriteria[filter_groups][0][filters][0][value]=24-MB0%&searchCriteria[filter_groups][0][filters][0][condition_type]=like

list of categories selected by search criteria
local.magento/index.php/rest/V1/categories/list?searchCriteria[filter_groups][0][filters][0][field]=is_active&
searchCriteria[filter_groups][0][filters][0][value]=false&searchCriteria[filter_groups][0][filters][0][condition_type]=eq


Within Magento’s root directory run below commands:
    Set Unsecure URL
    bin/magento setup:store-config:set --base-url="http://www.magento2.com/"
    Set Secure URL
    bin/magento setup:store-config:set --base-url-secure="https://www.magento2.com/"
    Clear Cache
    bin/magento cache:flush

backup mysql
docker exec docker-magento2_db_1 /usr/bin/mysqldump --password=myrootpassword -rbackfile.sql magento
docker cp docker-magento2_db_1:backfile.sql backfile.sql
restore mysql
docker cp backfile.sql proj2_db_1:backfile.sql
docker exec -i proj2_db_1 /usr/bin/mysql -uroot -pmyrootpassword magento < backfile.sql

create self-signed certificate (Power shell)
PS C:\Users\s.bovsunovskyi> New-SelfSignedCertificate -Type Custom -Subject "E=test@test.com,CN=Test Docker" -DnsName "localhost", "127.0.0.1", "192.168.99.100", "192.168.99.101" -CertStoreLocation "cert:\LocalMachine\My"