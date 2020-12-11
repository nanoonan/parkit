from setuptools import setup

setup(
  name='parkit',
  version='0.0.1',
  author='marvinsmind.com',
  author_email='nanoonan@marvinsmind.com',
  packages=['parkit'],
  scripts=[],
  url='http://pypi.python.org/pypi/parcolls/',
  license='LICENSE.txt',
  description='Toolkit for parallel applications.',
  long_description=open('README.md').read(),
  install_requires=[
    'mmh3',
    'pandas',
    'numpy',
    'lmdb',
    'daemoniker'
  ],
)