#### Python packaging for pypi 
NOTE: For internal use:

* Setup pip build system (only once):
```
python3 -m pip install --upgrade build
python3 -m pip install --upgrade twine
```

* Build locally: 
```
rm -rf dist/*
python3 -m build
```

* Uploading to pypi
``` 
python3 -m twine upload dist/*  
```

#### Contents & Rationale

This project structure adheres to the packaging requirements set forth by [the Python Package Index](https://packaging.python.org/tutorials/packaging-projects/).

* setup.py: has contents that are configured for PyPi, any new requirements that normally exist in requirements.txt must be in setup.py. Also, version incrementing happens in this file.

* pyproject.toml: contains build info for the client.

* Makefile: I like this here to assist in environment setup and utility use.

* structure:
  - src: this is where the main code is
  - tests: this contains the tests for the code
   