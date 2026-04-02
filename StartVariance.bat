@echo off
echo Starting local server on http://localhost:8000 ...
start http://localhost:8000/variance.html
python -m http.server 8000
