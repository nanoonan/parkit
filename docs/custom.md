## Custom Objects
You can create custom classes that automatically persist their attributes.

In this example, the lifetime of the 'local' attribute is the same as the object instance. The rule is that any attribute declared in the class definition is not persistent. The 'persistent' attribute is a transactional attribute and you can access it from multiple processes. 


```python
from parkit import (
    Dict,
    Object
)
```


```python
# Subclass Object to create a custom class with peristent, transactional attributes
class MyClass(Object):

    # This is not persistent
    local = 1

    def __init__(self, path):
        # Call the super __init__ first
        super().__init__(path)
        # Add a new persistent attribute if it doesn't exist. If we don't have this test
        # each new instance will overwrite the value of the persistent attribute.
        if 'persistent' not in self.attributes():
            self.persistent = 'foo'
        
    def hello(self):
        print('hello', self.persistent, self.local)
```


```python
# Create two named instances
x1 = MyClass('examples/foo1')
x2 = MyClass('examples/foo2')
```


```python
list(x1.attributes()), list(x2.attributes())
```




    (['persistent'], ['persistent'])




```python
# Change the attribute values for x1
x1.local = 2
x1.persistent = 'bar'
x1.hello()
x2.hello()
```

    hello bar 2
    hello foo 1
    


```python
# Create a new instance of x1. Note the local attribute is reset.
x1 = MyClass('examples/foo1')
x1.hello()
x2.hello()
```

    hello bar 1
    hello foo 1
    


```python
# References to other parkit objects are valid.
d = Dict('examples/mydict')
d['key'] = 'value'
x1.mydict = d
```


```python
x1 = MyClass('examples/foo1')
x1.mydict['key']
```




    'value'




```python
x1.drop()
x2.drop()
```


```python

```
