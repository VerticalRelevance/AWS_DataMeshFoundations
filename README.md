# AWS_DataMeshFoundations
Data mesh reference architectures for AWS.


## Manual Steps to Create Data Mesh

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



## Using this Project

### Folder Structure
The following outlines the folder structure of the project:
- src
    - accounts
        - central
        - consumer
        - producer
    - ops

### Developing with CDK Alpha Modules
CDK2 changes how alpha modules are used in project. For this project, the following additional modules must be installed with PIP in order for the CDK deployment to work properly:
`pip install aws-cdk.aws-glue-alpha`

Once this command is run, you should be able to run the deployments properly within this project. Additionally, note that the there is a distinct import command for working with these alpha modules:
```python
from aws_cdk import (
    Duration,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_lambda as lambda_
)
```
In the above imports, `aws_glue_alpha` is imported as `glue_alpha` and serves as a separate and distinct modules within the Python execution context. So, in the code, you use that particular module for your 'alpha' components. For example:
```python
database = glue_alpha.Database(self, id='my_database_id',
                        database_name='producer-a-db'
)
```

##Standalone Manual Steps

Cross account setup:  

The Central catalog account needs access to the Lake house data store of the producer accounts. Cross-account set up is required from the Central catalog account to the producer account. The following steps need to be taken to provide cross-account access: 

IAM policies and resource-based bucket policies 

Create an IAM role in Central catalog account 

Provide IAM role in central catalog account a permission to access lake house objects 

Configure a bucket policy in S3 in the producer account to grant access to the role created in the central catalog account 

 

After completion of the cross-account setup, the central catalog account can access the s3 objects in the data lake owned by the producer account. 

 

Central Catalog Configuration: 

Register Data Lake locations: Register the Lake house locations of the producer accounts in the central catalog account. As there is cross account access between the producer account and central catalog account, the Lake house location will be accessible from central catalog account. It is recommended to register the bucket so all objects in the bucket and its sub-directories are accessible. 

 

Create a Data Catalog: Create a data catalog database in the central catalog account. Create table catalogs in the catalog database. The table catalog can be created manually or using the Crawler 

 

Creating data catalog using Crawler: 

Add a policy in AWS Glue Catalog settings to access Lake house locations in the producer accounts. Set “EvaluatedByLakeFormationTags” property to true in the policy condition. It will enable LF tag-based access control 

Create a Glue Crawler to source data stores pointing to the lake house locations in the producer accounts and have the output pointing to the catalog database 

Manually creating data catalog: 

Create a generic lambda function to create a table catalog in the catalog database. Table metadata information is passed to the lambda function using a JSON file. 

 

Create and assign LF Tags: Based on the requirements, create Lake Formation tags in the central catalog account. LF tags are key-value pair property and multiple values can be assigned to key. Once the LF tags are created, assign these tags to catalog tables or columns. These LF tags will used to control access to catalog objects. 

 

Grant Permissions to Consumers: Grant Data Lake permissions to External Account (Consumer accounts) based on LF tags. Database and Table level permissions can be granted to consumer accounts. It is recommended to provide only table level permissions to consumer accounts. With these permissions, the central catalog database will be visible to the consumer account Lake formation admin. 

 

Consumer Account Configuration: 

Create a Resource Link: Once permissions are granted to consumer accounts from the central catalog account, the central catalog database objects will be visible to the admin of the consumer account. The Consumer Account admin has to create a resource link which will create a local catalog database in consumer account. 

 

Grant permission to consumer account users: The consumer account admin grants access to their users based on LF tags. The consumer account can inherit LF tags from central catalog account in addition to creating their own LF Tags. 

 