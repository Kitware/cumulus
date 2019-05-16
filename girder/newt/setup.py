from setuptools import setup, find_packages

setup(
    name='cumulus-newt',
    version='0.1.0',
    description='Allows login to Girder using a NEWT session id and access to files through a read-only assetstore.',
    packages=find_packages(),
    install_requires=[
      'girder>=3.0.0a5'
    ],
    entry_points={
      'girder.plugin': [
          'newt = newt:NewtPlugin'
      ]
    }
)
