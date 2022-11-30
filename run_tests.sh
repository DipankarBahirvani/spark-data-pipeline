set -e
check=""
export PYTHONPATH=batch_processing
echo $PYTHONPATH
black ${check}  tests/ batch_processing
isort ${check}  tests/ batch_processing/
mypy tests batch_processing
pylint tests/**.py batch_processing
pytest  tests