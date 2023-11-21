import json
import re
from typing import Any, Dict, List

import boto3
from onevizion import IntegrationLog, LogLevel, Trackor

from module_error import ModuleError


class OVAccessParameters:
    #region constants
    REGEXP_PROTOCOLS = '^(https|http)://'
    #endregion

    def __init__(self, ov_url: str, ov_access_key: str, ov_secret_key: str):
        self.ov_url_without_protocol = re.sub(OVAccessParameters.REGEXP_PROTOCOLS, '', ov_url)
        self.ov_access_key = ov_access_key
        self.ov_secret_key = ov_secret_key


class BLOBToS3FieldValues:

    def __init__(self, trackor_type: str, field_with_blob_ids: str, delimiter: str,
                 trigger: str, clear_trigger: Dict[str, Any]):
        self.trackor_type = trackor_type
        self.field_with_blob_ids = field_with_blob_ids
        self.delimiter = delimiter
        self.trigger = trigger
        self.clear_trigger = clear_trigger


class UpdatedBLOBs:

    def __init__(self, trackor_id: int, trackor_key: str, is_clear_trigger: bool):
        self.trackor_id = trackor_id
        self.trackor_key = trackor_key
        self.is_clear_trigger = is_clear_trigger


class BLOBToS3Trackor:
    # region constants
    IS_TRACKOR_ENABLED = 1
    # endregion

    def __init__(self, blob_to_s3_trackor_type: str,
                 blob_to_s3_fields: Dict[str, str],
                 ov_access_parameters: OVAccessParameters):
        self.trackor_type = blob_to_s3_trackor_type
        self.trackor_fields = BLOBToS3TrackorFields(blob_to_s3_fields)
        self._trackor_type_wrapper = Trackor(
            trackorType=blob_to_s3_trackor_type,
            URL=ov_access_parameters.ov_url_without_protocol,
            userName=ov_access_parameters.ov_access_key,
            password=ov_access_parameters.ov_secret_key,
            isTokenAuth=True
        )

    def get_trackors(self) -> List[Dict[str, Any]]:
        self._trackor_type_wrapper.read(
            filters={self.trackor_fields.status: BLOBToS3Trackor.IS_TRACKOR_ENABLED},
            fields=self.trackor_fields.get_trackor_fields()
        )

        if len(self._trackor_type_wrapper.errors) > 0:
            raise ModuleError('Failed to get_blob_to_s3_trackors', self._trackor_type_wrapper.errors)

        return list(self._trackor_type_wrapper.jsonData)

    def get_field_values(self, blob_to_s3_trackor: Dict[str, Any]) -> BLOBToS3FieldValues:
        return BLOBToS3FieldValues(blob_to_s3_trackor[self.trackor_fields.trackor_type],
                                   blob_to_s3_trackor[self.trackor_fields.field_with_blob_ids],
                                   blob_to_s3_trackor[self.trackor_fields.delimiter],
                                   blob_to_s3_trackor[self.trackor_fields.trigger],
                                   json.loads(blob_to_s3_trackor[self.trackor_fields.clear_trigger]))


class BLOBToS3TrackorFields:

    def __init__(self, blob_to_s3_fields: Dict[str, str]):
        self.trackor_type = blob_to_s3_fields['trackorType']
        self.field_with_blob_ids = blob_to_s3_fields['fieldWithBlobIds']
        self.delimiter = blob_to_s3_fields['delimiter']
        self.trigger = blob_to_s3_fields['trigger']
        self.clear_trigger = blob_to_s3_fields['clearTrigger']
        self.status = blob_to_s3_fields['status']

    def get_trackor_fields(self) -> List[str]:
        return [
            self.trackor_type, self.field_with_blob_ids,
            self.delimiter, self.trigger, self.clear_trigger
        ]


class OVTrackor:
    # region constants
    TRACKOR_ID = 'TRACKOR_ID'
    TRACKOR_KEY = 'TRACKOR_KEY'
    # endregion

    def __init__(self, ov_access_parameters: OVAccessParameters):
        self._ov_url_without_protocol = ov_access_parameters.ov_url_without_protocol
        self._ov_access_key = ov_access_parameters.ov_access_key
        self._ov_secret_key = ov_access_parameters.ov_secret_key
        self._trackor_type_wrapper = Trackor()

    @property
    def trackor_type_wrapper(self) -> Trackor:
        return self._trackor_type_wrapper

    @trackor_type_wrapper.setter
    def trackor_type_wrapper(self, trackor_type_name: str):
        self._trackor_type_wrapper = Trackor(
            trackorType=trackor_type_name, URL=self._ov_url_without_protocol,
            userName=self._ov_access_key, password=self._ov_secret_key,
            isTokenAuth=True
        )

    def get_trackors_by_fields_and_search_trigger(self, fields_list: List[str],
                                                  search_trigger: str) -> List[Dict[str, Any]]:
        self.trackor_type_wrapper.read(
            fields=fields_list,
            search=search_trigger
        )

        if len(self.trackor_type_wrapper.errors) > 0:
            raise ModuleError('Failed to get_trackors_by_fields_and_search_trigger', self.trackor_type_wrapper.errors)

        return list(self.trackor_type_wrapper.jsonData)

    def clean_trackor_trigger_by_filters(self, trackor_key: str,
                                         filter_dict: Dict[str, Any],
                                         field_dict: Dict[str, Any]):
        self.trackor_type_wrapper.update(
            filters=filter_dict,
            fields=field_dict
        )

        if len(self.trackor_type_wrapper.errors) > 0:
            raise ModuleError(
                f'Failed to clean_trackor_trigger_by_filters for {trackor_key}',
                self.trackor_type_wrapper.errors
            )


class AWSS3Service:
    # region constants
    S3_SERVICE_NAME = 's3'
    RESPONSE_METADATA = 'ResponseMetadata'
    HTTP_HEADERS = 'HTTPHeaders'
    CONTENT_TYPE = 'content-type'
    CONTENT_DISPOSITION = 'content-disposition'
    METADATA_DIRECTIVE = 'REPLACE'
    FILE_NAME_TEMPLATE = 'blob_data/{blob_data_id}'
    BUCKET_KEY_DICT = 'Bucket'
    KEY_KEY_DICT = 'Key'
    #endregion

    def __init__(self, aws_s3_parameters: Dict[str, str]):
        self._source_bucket = aws_s3_parameters['sourceBucket']
        self._destination_bucket = aws_s3_parameters['destinationBucket']
        self._s3_client = boto3.client(
            AWSS3Service.S3_SERVICE_NAME,
            region_name=aws_s3_parameters['region'],
            aws_access_key_id= aws_s3_parameters['accessKey'],
            aws_secret_access_key = aws_s3_parameters['secretKey']
        )
        self._s3_resource = boto3.resource(
            AWSS3Service.S3_SERVICE_NAME,
            region_name=aws_s3_parameters['region'],
            aws_access_key_id= aws_s3_parameters['accessKey'],
            aws_secret_access_key = aws_s3_parameters['secretKey']
        )

    def copy_from_source_to_destination_bucket(self, blob_data_id: int):
        file_name = AWSS3Service.FILE_NAME_TEMPLATE.format(blob_data_id)

        s3_http_headers = self._get_s3_http_headers(file_name)
        obj = self._s3_resource.Object(Bucket=self._destination_bucket,
                                       Key=file_name)
        obj.copy_from(CopySource={AWSS3Service.BUCKET_KEY_DICT: self._source_bucket,
                                  AWSS3Service.KEY_KEY_DICT: file_name},
                      MetadataDirective=AWSS3Service.METADATA_DIRECTIVE,
                      ContentType=s3_http_headers[AWSS3Service.CONTENT_TYPE],
                      ContentDisposition=s3_http_headers[AWSS3Service.CONTENT_DISPOSITION])

    def _get_s3_http_headers(self, file_name: str):
        s3_head_object = self._s3_client.head_object(Bucket=self._source_bucket,
                                                     Key=file_name)
        return s3_head_object[AWSS3Service.RESPONSE_METADATA][AWSS3Service.HTTP_HEADERS]


class Module:

    def __init__(self, ov_module_log: IntegrationLog, settings_data: Dict[str, Any]):
        _ov_access_parameters = OVAccessParameters(settings_data['ovUrl'],
                                                   settings_data['ovAccessKey'],
                                                   settings_data['ovSecretKey'])
        self._blob_to_s3_trackor = BLOBToS3Trackor(settings_data['BLOBToS3TrackorType'],
                                                   settings_data['BLOBToS3Fields'],
                                                   _ov_access_parameters)
        self._aws_s3_service = AWSS3Service(settings_data['AWS'])
        self._ov_trackor = OVTrackor(_ov_access_parameters)
        self._module_log = ov_module_log

    def start(self):
        self._module_log.add(LogLevel.INFO, 'Starting Module')

        blob_to_s3_trackors = self._blob_to_s3_trackor.get_trackors()
        self._module_log.add(LogLevel.INFO,
                             f'{len(blob_to_s3_trackors)} {self._blob_to_s3_trackor.trackor_type} Trackors found.')

        for blob_to_s3_trackor in blob_to_s3_trackors:
            blob_to_s3_field_values = self._blob_to_s3_trackor.get_field_values(blob_to_s3_trackor)
            blob_to_s3_field_with_blob_ids = blob_to_s3_field_values.field_with_blob_ids

            self._ov_trackor.trackor_type_wrapper = blob_to_s3_field_values.trackor_type
            trackors_with_blobs = self._ov_trackor.get_trackors_by_fields_and_search_trigger(self._get_fields_list(blob_to_s3_field_with_blob_ids),
                                                                                             blob_to_s3_field_values.trigger)
            trackors_to_clear_trigger = self._copy_blob_ids(trackors_with_blobs,
                                                            blob_to_s3_field_with_blob_ids,
                                                            blob_to_s3_field_values.delimiter)
            self._clear_trackors_trigger(trackors_to_clear_trigger,
                                         blob_to_s3_field_values.clear_trigger)

        self._module_log.add(LogLevel.INFO, 'Module has been completed')

    def _get_fields_list(self, blob_to_s3_field_with_blob_ids: str) -> List[Any]:
        fields_list = []
        fields_list.append(blob_to_s3_field_with_blob_ids)
        fields_list.append(OVTrackor.TRACKOR_KEY)
        return fields_list

    def _copy_blob_ids(self, trackors_with_blobs: List[Dict[str, Any]],
                       field_with_blob_ids: str,
                       delimiter: str) -> List[UpdatedBLOBs]:
        trackors_to_clear_trigger = []
        for trackor_with_blob in trackors_with_blobs:
            is_clear_trigger = True
            trackor_id = trackor_with_blob[OVTrackor.TRACKOR_ID]
            trackor_key = trackor_with_blob[OVTrackor.TRACKOR_KEY]
            trackor_field_with_blob_ids = trackor_with_blob[field_with_blob_ids]

            if trackor_field_with_blob_ids is None:
                self._module_log.add(LogLevel.INFO,
                                     f'Field with BLOB ID is empty for {trackor_key}. The trigger will still be cleared.')
            else:
                blob_ids = trackor_field_with_blob_ids.split(delimiter)
                self._module_log.add(LogLevel.INFO,
                                     f'{len(blob_ids)} BLOB IDs found for {trackor_key}')

                for blob_id in blob_ids:
                    try:
                        self._aws_s3_service.copy_from_source_to_destination_bucket(blob_id)
                        self._module_log.add(LogLevel.INFO,
                                             f'{blob_id} BLOB ID successfully copied')
                    except ModuleError as module_error:
                        self._module_log.add(LogLevel.ERROR,
                                             f'Failed to copy_from_source_to_destination_bucket for {blob_id} BLOB ID',
                                             module_error)
                        is_clear_trigger = False

            trackors_to_clear_trigger.append(UpdatedBLOBs(trackor_id,
                                                          trackor_key,
                                                          is_clear_trigger))

        return trackors_to_clear_trigger

    def _clear_trackors_trigger(self, trackors_to_clear_trigger: List[UpdatedBLOBs],
                                clear_trigger: Dict[str, Any]):
        for trackor_to_clear_trigger in trackors_to_clear_trigger:
            trackor_key = trackor_to_clear_trigger.trackor_key
            if trackor_to_clear_trigger.is_clear_trigger:
                trackor_filter = {self._ov_trackor.TRACKOR_ID: trackor_to_clear_trigger.trackor_id}
                self._ov_trackor.clean_trackor_trigger_by_filters(trackor_key,
                                                                  trackor_filter,
                                                                  clear_trigger)
            else:
                self._module_log.add(LogLevel.INFO,
                                     f'Trigger has not been cleared for {trackor_key}')
