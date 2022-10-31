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

## Running The BluePrint
1. Download aws keys for 3 separate accounts into named AWS profiles on your local machine "producer", "central", and "consumer"
2. Create a python virtual environment, and install the dependencies in src/ops/requirements.txt, and activate this env
3. in the code structure go to the src/ops and run "deploy-all.sh"
4. login to the console and setup the SSO user that you downloaded credentials for to the "Data lake admin" list in the lakeformation section.
5. Add the SSO user that you downloaded credentials fore to the "Database Creators" list in lakeoformation section of the console. 
6. Also do some first time setup for lakeformation to remove the "IAM-only" credential settings so that TBAC works properly. These changes should be prompted/alerted by the console when you enter the lakeformation UI
7. perform steps 4 - 6 on both the central and consumer accounts
8. run the post-setup python "python src/ops/post_setup.py"

## Known issues with current blueprint setup
1. After all setup shown here is done, there are still some permission issues with the SSO user being unable to query the linked table in the consumer account. 

## Next Steps for blueprint development
1. All of the things that are done in the post_setup.py should be moved to CDK if possible
2. Setup for consumer & Producer account should be refactored to work as a service catalog product. This service catalog product should be maintained in the central account, then shared with the consumer & producer accounts. Adding new data sources to a producer or consumer could potentially be added as service actions to these products as well.
