from jupyter/pyspark-notebook
copy dist/*/*.whl /tmp
run pip install /tmp/*whl
copy dist/etc /home/jovyan/etc


