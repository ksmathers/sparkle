from jupyter/pyspark-notebook
#copy dist/*/*.whl /tmp
copy dist/sparkle/*whl /tmp
copy dist/dca-aws-jupyter/*whl /tmp
run pip install /tmp/*whl
copy dist/etc /home/jovyan/etc


