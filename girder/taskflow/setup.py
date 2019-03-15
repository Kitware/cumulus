from setuptools import setup, find_packages

setup(
    name='cumulus-taskflow',
    version='0.1.0',
    description='RESTful endpoints for managing flows of Celery tasks.',
    packages=find_packages(),
    install_requires=[
      'girder>=3.0.0a5',
      'cumulus-plugin'
    ],
    entry_points={
      'girder.plugin': [
          'taskflow = taskflow:TaskFlowPlugin'
      ]
    }
)
