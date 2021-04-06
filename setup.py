from setuptools import setup

setup(
  name='parkit',
  version='0.0.1',
  author='marvinsmind.com',
  author_email='nanoonan@marvinsmind.com',
  packages=['parkit'],
  scripts=[],
  url='http://pypi.python.org/pypi/parkit/',
  license='LICENSE.txt',
  description='Useful classes for inter-process programming.',
  long_description=open('README.md').read(),
  install_requires=[
    'mmh3',
    'lmdb',
    'daemoniker',
    'psutil',
    'orjson'
  ],
)