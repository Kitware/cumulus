from setuptools import setup, find_packages

setup(
    name="cumulus",
    version="0.1.0",
    description="Girder API endpoints for interacting with cloud providers.",
    author="Chris Haris",
    author_email="chris.harris@kitware.com",
    url="https://github.com/Kitware/cumulus",
    packages=find_packages(exclude=["*.tests", "*.tests.*",
                                    "tests.*", "tests"]),
    package_data={
        "": ["*.json", "*.sh"],
        "cumulus": ["conf/*.json", "templates/*.sh", "templates/*/*.sh"],
    })
