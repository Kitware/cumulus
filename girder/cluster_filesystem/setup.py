from setuptools import setup, find_packages

setup(
    name='cumulus-cluster-filesystem',
    version='0.1.0',
    description='Provides remote file browsing capabilities for cluster filesystems.',
    packages=find_packages(),
    install_requires=[
      'girder>=3.0.0a5',
      'cumulus',
      'cumulus-plugin',
      'easydict==1.5',
      'paramiko==2.4.2'
    ],
    entry_points={
      'girder.plugin': [
          'cluster_filesystem = cluster_filesystem:ClusterFileSystemPlugin'
      ]
    }
)
