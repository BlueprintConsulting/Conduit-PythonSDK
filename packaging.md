#### Python packaging for pypi 
NOTE: For internal use:

* Build locally: 
```
python3 -m pip install --upgrade build
python3 -m build
```

* Uploading to pypi
```
python3 -m pip install --upgrade twine 
python3 -m twine upload dist/*  
```