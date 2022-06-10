# AWS_DataMeshFoundations
Data mesh reference architectures for AWS.


## Steps to Create Data Mesh

* Create the producer, consumer, and central catalog account 

&nbsp;

__Producer Account:__  

* Create an S3 bucket. Load data into the bucket. Sample data can be found in the sample data folder of the repo. 

&nbsp;
&nbsp;
&nbsp;
 
__Central Catalog Account:__  

* Establish a Dake Administrator role in IAM and in Lake Formation 

* Create a Glue Crawler role that has permission to access S3, and attach the Glue Service Role managed policy, and Lake Formation Data Admin policy. An example policy can be found in the supplementary file folder of the repo. 

* Create a Glue Catalog Permissions Policy that allows the producer and consumer accounts to access data in the central catalog account though lake formation tags. This ensures the data cannot be shared to these accounts unless permission is granted in Lake Formation though a tag. An example policy can be found in the supplementary file folder of the repo. 

&nbsp; 
&nbsp;
&nbsp;

__Producer Account:__

* Add a bucket policy to the sample data bucket to allow access to the glue crawler role and the Data Lake administrator role. 

&nbsp;
&nbsp;
&nbsp;

__Central Catalog Account:__

* Run the Glue Crawler, tables should now appear in Lake Formation.  

* Assign tags to the database, tables, and columns based on required criteria. 

* Under Lake Formation Permissions, grant appropriate access to the Producer and Consumer accounts through Lake Formation Tags.  

&nbsp;
&nbsp;
&nbsp;
 

__Producer and Consumer Account:__

* Create a resource link that points to the shared databases/tables from the central catalog account. 

* Create an S3 bucket to store Athena queries. 

