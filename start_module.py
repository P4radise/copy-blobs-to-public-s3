import subprocess
import sys

installed_dependencies = subprocess.check_output([sys.executable, '-m', 'pip', 'install', '-r', 'python_dependencies.ini']).decode().strip()
if 'Successfully installed' in installed_dependencies:
    raise Exception('Some required dependent libraries were installed. ' \
                    'Module execution has to be terminated now to use installed libraries on the next scheduled launch.')

import json
import re

from jsonschema import validate
from onevizion import IntegrationLog, LogLevel

from module import Module
from module_error import ModuleError

with open('settings.json', 'rb') as settings_file:
    settings_data = json.loads(settings_file.read().decode('utf-8'))

with open('settings_schema.json', 'rb') as settings_schema_file:
    settings_schema = json.loads(settings_schema_file.read().decode('utf-8'))

try:
    validate(instance=settings_data, schema=settings_schema)
except ModuleError as module_error:
    raise ModuleError('Incorrect value in the settings file', str(module_error)) from module_error

with open('ihub_parameters.json', 'rb') as ihub_parameters_file:
    module_data = json.loads(ihub_parameters_file.read().decode('utf-8'))

ov_url = re.sub('^(https|http)://', '', settings_data['ovUrl'])
process_id = module_data['processId']
log_level = module_data['logLevel']

module_log = IntegrationLog(
    process_id, ov_url, settings_data['ovAccessKey'],
    settings_data['ovSecretKey'], None, True, log_level
)
module = Module(module_log, settings_data)

try:
    module.start()
except ModuleError as module_error:
    module_log.add(LogLevel.ERROR, str(module_error))
    raise module_error
