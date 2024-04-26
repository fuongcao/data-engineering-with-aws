## AWS GLUE Configuration
<img alt="glue configuration" src="./iac_images/glue_configuration.PNG" width=75% height=75%>



### 1. Create bucket and  update landing data store to s3
```
aws s3 mb s3://prj3-stedi
aws s3 cp .\starter\ s3://prj3-stedi/ --recursive --exclude "*.md"
```
<img alt="create s3" src="./iac_images/create_s3.PNG" width=75% height=75%>

``` 
#### Create VPC endpoint
aws ec2 describe-vpcs
```
<img alt="aws ec2 describe-vpcs" src="./iac_images/vpc_describes.png" width=45% height=45%>

```
aws ec2 describe-route-tables
```
<img alt="aws ec2 describe-route-tables" src="./iac_images/vpc_describe-route-tables.png" width=45% height=45%>

```
aws ec2 create-vpc-endpoint --vpc-id vpc-0230a32badedc6071 --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-051ee8abd78 fdbcf2
```

<img alt="aws ec2 create-vpc-endpoint" src="./iac_images/create_vpc_s3endpoint.png" width=75% height=75%>
<img alt="aws ec2 create-vpc-endpoint" src="./iac_images/create_vpc_s3endpoint_console.png" width=75% height=75%>

### 2. Creating the Glue Service Role
Create project3GlueServiceRole with Trusted Policy [glue_trusted_policy.json](iam_role/glue_trusted_policy.json)
```
aws iam create-role --role-name project3GlueServiceRole --assume-role-policy-document file://iam_role/glue_trusted_policy.json
```
<img alt="create_glue_service_role" src="./iac_images/create_glue_service_role.png" width=75% height=75%>


Grant Glue Privileges on the S3 Bucket [s3_data_source_access_policy.json](iam_role/s3_data_source_access_policy.json)
```
aws iam put-role-policy --role-name project3GlueServiceRole --policy-name S3Access --policy-document file://iam_role/s3_data_source_access_policy.json
```

<img alt="grant_glue_service_s3_access_policy" src="./iac_images/grant_glue_service_s3_access_policy.png" width=75% height=75%>

Last, we need to give project3GlueServiceRole access to data in special S3 buckets used for Glue configuration, and several other resources.
```
aws iam put-role-policy --role-name project3GlueServiceRole --policy-name GlueAccess --policy-document file://iam_role/glue_resources_access_policy.json
```
<img alt="grant_glue_resources_access_policy" src="./iac_images/grant_glue_resources_access_policy.png" width=75% height=75%>