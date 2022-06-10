# AWS_DataMeshFoundations
Data mesh reference architectures for AWS.



Accounts are set up (producer, consumer, and central catalog) 

Producer account has data in the refined bucket which is shared with the central catalog account glue crawler role through the bucket policy 

Central Catalog account crawls the bucket in the producer account to create the catalog tables 

The Central Catalog database and tables are assigned tags 

Central Catalog grants permissions to the producer and consumer account through the following: 

  * Glue Catalog policy is created to allow other accounts to access catalog objects through lake formation tags only 

  * Lake Formation grants are given to each account based on tags, so the appropriate databases/tables now show up in the producer and consumer accounts 

The producer and consumer accounts have resource links pointing to the shared databases/tables from the central catalog account 
