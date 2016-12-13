# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing class for GCP's Cloud Dataflow service."""

import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dataflow_service

from subprocess import check_call

FLAGS = flags.FLAGS

archetype_cmd = """mvn archetype:generate \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=LATEST \
      -DarchetypeGroupId=org.apache.beam \
      -DgroupId=org.example \
      -DartifactId=word-count-beam-{0} \
      -Dversion="0.1" \
      -DinteractiveMode=false \
      -Dpackage=org.apache.beam.examples \
"""

run_cmd = """mvn exec:java \
      -Dexec.mainClass={0} \
      -Dexec.args="--runner=BlockingDataflowRunner \
            --gcpTempLocation=gs://{1}/tmp \
            --inputFile={2} \
            --output=gs://{3}/counts"
"""


class GcpDataflow(dataflow_service.BaseDataflowService):
  """Object representing GCP Dataflow."""

  CLOUD = providers.GCP
  SERVICE_NAME = 'dataflow'

  def __init__(self, dataflow_service_spec):
    super(GcpDataflow, self).__init__(dataflow_service_spec)

  @staticmethod
  def _GetStats(stdout):
    return {}
  
  def _Create(self):
    self.tmp_num = time.time()
    check_call([archetype_cmd.format(self.tmp_num)], cwd='/tmp', shell=True)
    check_call(['mvn compile'],
               cwd='/tmp/word-count-beam-{0}'.format(self.tmp_num),
               shell=True)

  def _Delete(self):
    check_call(['rm', '-r', '/tmp/word-count-beam-{0}'.format(self.tmp_num)])
    pass

  def _exists(self):
    return True

  def SubmitJob(self, class_name, input_file, staging_bucket, output_bucket,
                job_stdout_file=None, job_arguments=None, version='nightly'):
    check_call([run_cmd.format(class_name, staging_bucket, input_file, output_bucket)],
               cwd='/tmp/word-count-beam-{0}'.format(self.tmp_num),
               shell=True)
    return {"success": True}
  
  def SetClusterProperty(self):
    pass
