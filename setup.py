from setuptools import setup, find_packages

setup(
    name='masschange',
    version='0.1',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    # package_data={'ingest': ['.inputs/Shapefiles/California/*']}
)