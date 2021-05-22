import os
import subprocess

directory = os.fsencode('.')

for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith('ipynb'):
        print('converting', filename)
        subprocess.run(['jupyter', 'nbconvert', '--to', 'markdown', filename])
