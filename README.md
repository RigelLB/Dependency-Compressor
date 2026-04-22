## Overview
Meant to be able to take in a file containing pypi packages style requirements and return a dependency resolved file that contains available versions defined by the separator in the package description. It will also save a state of the dependency graph for the next run. It will first check to see if the input file is different than the last run. If new packages are added it will simply add the package and update the final output. If packages are modified or deleted it will rebuild the graph.

## Usage
Modify the constants at the top of the main.py file to reflect the files you would like to work with.

#### Ex: 
- Having the package defined as librosa>=0.9.0 will:
  1. Attempt to get all versions starting at 0.9.0
  2. Fetch all dependencies for all versions
  
- Having the package defined as librosa==0.9.0 will:
  1. Only fetch the dependencies for that specific version

- Having the package defined as librosa will:
  1. Only fetch the newest version available and its dependencies 
   
## Future Additions
- In flight control should be added to avoid massive memory ballooning if the commands fail prematurely.
