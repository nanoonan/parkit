import cloudpickle
import logging
import parkit.constants as constants

from parkit.adapters.attribute import Attr
from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class ProcessProxy(Object):
  
  process = Attr()

class Process(Object):  

  pid = Attr(readonly = True)

  status = Attr(readonly = True)
  
  error = Attr(readonly = True)
  
  node_uid = Attr(readonly = True)
  
  result = Attr(readonly = True)

  def __init__(
    self, path, create = True, bind = True, versioned = False, 
    target = None, args = (), kwargs = {}
  ): 
    
    def on_create(metadata):
      self._put_attr('_pid', None)
      self._put_attr('_result', None)
      self._put_attr('_error', None)
      self._put_attr('_target', target)
      self._put_attr('_args', args)
      self._put_attr('_kwargs', kwargs)
      self._put_attr('_status', ['submitted'])
      self._put_attr('_node_uid', None)
      proxy = ProcessProxy(
        '/'.join([PROCESS_NAMESPACE, self.uuid]), create = True, bind = False,
        versioned = False
      )
      proxy.process = self

    super().__init__(
      path, create = create, bind = bind, versioned = versioned, on_create = on_create
    )
      
  def drop(self):
    with context(self, write = True, inherit = True):
      super().drop()
      proxy = ProcessProxy(
        '/'.join([PROCESS_NAMESPACE, self.uuid]), create = False, bind = True
      )
      proxy.drop()

  def attr_encode_value(self, value):
    return cloudpickle.dumps(value)

  def attr_decode_value(self, value):
    return cloudpickle.loads(value)

  