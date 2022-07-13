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