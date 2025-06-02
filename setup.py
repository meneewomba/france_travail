from setuptools import setup, find_packages

# pip install -e .
# This will install the package in "editable" mode, meaning changes to the code will be reflected immediately without needing to reinstall.
setup(
    name="jun24_cde_job-market",  # Package name
    version="0.1",  # Package version
    packages=find_packages(where="."),  # Finds all the packages in the current directory (i.e., the root "jun24" folder)
    package_dir={'': '.'},  # Indicates that the root package is in the current directory
)
