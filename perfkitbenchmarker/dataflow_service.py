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

"""Benchmarking support for Google Cloud Dataflow services.

This class contains options which allow for benchmarking of Apache Beam
against the Google Cloud Dataflow service.

""" 

import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

PROVIDER_MANAGED = 'managed'

_DATAFLOW_SERVICE_REGISTRY = {}

def enum(**enums):
  return type('Enum', (), enums)

BEAM_VERSION = enum(HEAD='head', NIGHTLY='nightly')

def GetDataflowServiceClass(cloud, service_type):
  """Get the Dataflow class corresponding to 'cloud'."""
  if cloud in _DATAFLOW_SERVICE_REGISTRY:
    return _DATAFLOW_SERVICE_REGISTRY.get(cloud)
  else:
    raise Exception('No Dataflow service found for {0}'.format(cloud))


class AutoRegisterDataflowServiceMeta(abc.ABCMeta):
  """Metaclass which allows DataflowServices to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('BaseDataflowService subclasses must have a CLOUD'
                        'attribute.')
      else:
        _DATAFLOW_SERVICE_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterDataflowServiceMeta, cls).__init__(name, bases, dct)


class BaseDataflowService(resource.BaseResource):
  """Object representing a Dataflow Service."""

  __metaclass__ = AutoRegisterDataflowServiceMeta

  def __init__(self, dataflow_service_spec):
    super(BaseDataflowService, self).__init__(user_managed=False)
    self.spec = dataflow_service_spec

  @abc.abstractmethod
  def SubmitJob(self, class_name, job_stdout_file=None, job_arguments=None,
                  version=BEAM_VERSION.NIGHTLY):
    """Submit a job to the Dataflow service.

    Submits a job and waits for it to complete.

    Args:
      class_name: Name of the main class.
      job_stdout_file: String giving the location of the file in
        which to put the standard out of the job.
      job_arguments: Arguments to pass to class_name.  These are
        not the arguments passed to the wrapper that submits the
        job.
      version: Beam version to use for this benchmark.

    Returns:
      dictionary, where success is true if the job succeeded,
      false otherwise.  The dictionary may also contain an entry for
      running_time and pending_time if the platform reports those
      metrics.
    """
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    return {}
