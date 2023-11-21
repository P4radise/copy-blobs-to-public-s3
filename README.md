# copy-blobs-to-public-s3

Module to create a copy of an object that is already stored in Amazon S3.

## Usage
1. Install module
2. Create and fill VHMBLOBTOS3 Trackor Type.
    * In VHMBLOBTOS3, you need to fill in the following fields
      | Field | Description | Example |
      | --- | --- | --- |
      | VHMBLOBTOS3:Trackor Type | Here you should specify the Trackor Type from which you want to get BLOB IDs
      | VHMBLOBTOS3:Field with BLOB IDs | Here you should specify the name of the field storing BLOB IDs
      | VHMBLOBTOS3:Delimiter for splitting BLOB IDs | Here you should specify a delimiter if the field can store more than one BLOB ID. By default, values are separated by a space character. If only one BLOB ID will be stored in the field, you do not need to fill this field. | If the field is empty, the BLOB IDs should look like this - 12345 67890. If the BLOB IDs are specified as 12345, 67890, the separator should be ", " (comma and space character).
      | VHMBLOBTOS3:Trigger | This should contain the field and its value by which the received Trackors will be filtered. The value should be in equal(FIELD_NAME,FIELD_VALUE) format. | equal(COPY_BLOBS_TO_S3,1) equal(COPY_BLOBS_TO_S3,"text")
      | VHMBLOBTOS3:Clean Trigger | This should contain a field and its value, which will reset the filter if all BLOB IDs of this Trackor are copied. The value should be in the format {"FIELD_NAME": FIELD_VALUE}. | {"COPY_BLOBS_TO_S3":0} {"COPY_BLOBS_TO_S3":"text"}
      | VHMBLOBTOS3:Enabled | For the module to consider this VHMBLOBTOS3 Trackor, it should be enabled
3. Create a user for this module.
   
   You can assign the role VHMBLOBTOS3_Administrator to grant access to all VHMBLOBTOS3 components. But you should also grant this user READ and EDIT permissions for those components that have been specified in the VHMBLOBLOBTOS3 Trackor Type.

4. Fill out the module settings file. See the example below, all of the required fields that you needed to fill out in step 2 are already contained there, you only need to fill in ovUrl, ovAccessKey and ovSecretKey and all the data for AWS.

Example of settings.json

```json
{
    "ovUrl": "https://***.onevizion.com",
    "ovAccessKey": "******",
    "ovSecretKey": "************",

    "BLOBToS3TrackorType": "VHMBLOBTOS3",
    "BLOBToS3Fields": {
        "trackorType": "VHMBLOBTOS3_TRACKOR_TYPE",
        "fieldWithBlobIds": "VHMBLOBTOS3_FIELD_WITH_BLOB_ID",
        "delimiter": "VHMBLOBTOS3_DELIMITER",
        "trigger": "VHMBLOBTOS3_TRIGGER",
        "clearTrigger": "VHMBLOBTOS3_CLEAN_TRIGGER",
        "status": "VHMBLOBTOS3_ENABLED"
    },

    "AWS": {
        "accessKey": "******",
        "secretKey": "************",
        "region": "us-east-1",

        "sourceBucket": "***.onevizion.com",
        "destinationBucket": "public-***.onevizion.com"
    }
}
```