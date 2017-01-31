# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs the word count job on data processing backends.

WordCount example reads text files and counts how often words occur. The input
is text files and the output is text files, each line of which contains a word
and the count of how often it occurs, separated by a tab.
The disk size parameters that are being passed as part of vm_spec are actually
used as arguments to the dpb service creation commands and the concrete
implementations (dataproc, emr, dataflow, etc.) control using the disk size
during the cluster setup.

dpb_wordcount_out_base: The output directory to capture the word count results

For dataflow jobs, please build the dpb_dataflow_jar based on
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven
"""

import copy
import datetime
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'beam_integration_benchmark'

BENCHMARK_CONFIG = """
beam_integration_benchmark:
  description: Run word count on dataflow and dataproc
  dpb_service:
    service_type: dataflow
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
"""

flags.DEFINE_string('dpb_it_class', 'org.apache.beam.examples.WordCountIT', 'Path to IT class')
flags.DEFINE_string('dpb_it_args', None, 'Args to provide to the IT')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS.dpb_it_args is None:
    raise errors.Config.InvalidValue('No args provided.')


def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):
  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='dpb_wordcount_benchmark',
                                            delete=False)
  stdout_file.close()

  # Switch the parameters for submit job function of specific dpb service
  job_arguments = ['"{}"'.format(arg) for arg in FLAGS.dpb_it_args.split(',')]
  classname = FLAGS.dpb_it_class

  if dpb_service_instance.SERVICE_TYPE == dpb_service.DATAFLOW:
    job_type = BaseDpbService.DATAFLOW_JOB_TYPE
  else:
    raise NotImplementedError('Currently only works against Dataflow.')

  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())

  jarfile = ''
  start = datetime.datetime.now()
  dpb_service_instance.SubmitJob(jarfile, classname,
                                 job_arguments=job_arguments,
                                 job_stdout_file=stdout_file,
                                 job_type=job_type)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass

