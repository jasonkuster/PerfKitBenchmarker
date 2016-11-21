# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a jar using a cluster that supports Cloud Dataflow.
"""

import datetime
import logging
import os
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import flags



BENCHMARK_NAME = 'dataflow'
BENCHMARK_CONFIG = """
dataflow:
  description: Run a jar on a dataflow cluster.
  dataflow_service:
    cloud: GCP
    service_type: managed
"""

# This points to a file on the dataflow cluster.
DEFAULT_CLASSNAME = 'org.apache.beam.examples.WordCount'

flags.DEFINE_string('beam_jarfile', None,
                    'If none, use the default Beam jar.')
flags.DEFINE_string('beam_classname', DEFAULT_CLASSNAME,
                    'Classname to be used')
flags.DEFINE_bool('dataflow_print_stdout', True, 'Print the standard '
                  'output of the job')
flags.DEFINE_list('dataflow_job_arguments', [], 'Arguments to be passed '
                  'to the class given by beam_classname')
flags.DEFINE_string('staging_bucket', None, 'Bucket for staging files')
flags.DEFINE_string('output_bucket', None, 'Bucket to store output')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):
  """Executes the given jar on the specified Spark cluster.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  if FLAGS.staging_bucket is None or FLAGS.output_bucket is None:
    raise Exception('staging_bucket and output_bucket must be supplied.')
  dataflow_cluster = benchmark_spec.dataflow_service
  jar_start = datetime.datetime.now()

  stdout_path = None
  results = []
  try:
    if FLAGS.dataflow_print_stdout:
      # We need to get a name for a temporary file, so we create
      # a file, then close it, and use that path name.
      stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                                prefix='dataflow_benchmark',
                                                delete=False)
      stdout_path = stdout_file.name
      stdout_file.close()

    stats = dataflow_cluster.SubmitJob(FLAGS.beam_classname,
                                       FLAGS.staging_bucket,
                                       FLAGS.output_bucket,
                                       job_stdout_file=stdout_path,
                                       job_arguments=FLAGS.dataflow_job_arguments)
    if not stats['success']:
      raise Exception('Class {0} did not run'.format(
          FLAGS.beam_classname))
    jar_end = datetime.datetime.now()
    if stdout_path:
      with open(stdout_path, 'r') as f:
        logging.info('The output of the job is ' + f.read())
    metadata = dataflow_cluster.GetMetadata()
    metadata.update({'class': FLAGS.beam_classname,
                     'job_arguments': str(FLAGS.dataflow_job_arguments),
                     'print_stdout': str(FLAGS.dataflow_print_stdout)})

    results.append(sample.Sample('wall_time',
                                 (jar_end - jar_start).total_seconds(),
                                 'seconds', metadata))

  finally:
    if stdout_path and os.path.isfile(stdout_path):
      os.remove(stdout_path)

  return results


def Cleanup(benchmark_spec):
  pass
