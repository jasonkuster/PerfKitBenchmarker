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

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dataflow_service
from perfkitbenchmarker.providers.gcp import util

from subprocess import check_call, check_output

FLAGS = flags.FLAGS


class GcpDataflow(dataflow_service.BaseDataflowService):
  """Object representing GCP Dataflow."""

  CLOUD = providers.GCP
  SERVICE_NAME = 'dataflow'

  def __init__(self, dataflow_service_spec):
    super(GcpDataflow, self).__init__(dataflow_service_spec)
    self.project = self.spec.worker_group.vm_spec.project
  
  @staticmethod
  def _GetStats(stdout):
    return {}
  
  def _Create(self):
    check_call(['mvn archetype:generate -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples -DarchetypeVersion=LATEST -DarchetypeGroupId=org.apache.beam -DgroupId=org.example -DartifactId=word-count-beam -Dversion="0.1" -DinteractiveMode=false -Dpackage=org.apache.beam.examples'], shell=True)
    check_call(['mvn compile'], cwd='/Users/jasonkuster/github/perfkitbenchmarker/word-count-beam', shell=True)

  def _Delete(self):
    check_call(['rm', '-r', 'word-count-beam'])
    pass

  def _exists(self):
    return True

  def SubmitJob(self, class_name, job_stdout_file=None, job_arguments=None,
                  version='nightly'):
    check_call(['mvn exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--runner=BlockingDataflowRunner --gcpTempLocation=gs://jasonkuster-temp/tmp --inputFile=gs://apache-beam-samples/shakespeare/* --output=gs://jasonkuster-temp/counts"'],
               cwd='/Users/jasonkuster/github/perfkitbenchmarker/word-count-beam',
               shell=True)
    return {"we did it": "Yay"}
  
  def SetClusterProperty(self):
    pass
