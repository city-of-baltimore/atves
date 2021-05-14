from setuptools import setup, find_packages

setup(
    name="atves",
    version="0.1",
    author="Brian Seel",
    author_email="brian.seel@baltimorecity.gov",
    description="Wrapper around the data sources used by the automated enforcement program in Baltimore",
    packages=find_packages('src'),
    package_data={'atves': ['py.typed'], },
    python_requires='>=3.0',
    package_dir={'': 'src'},
    install_requires=[
        'requests~=2.25.1',
        'xlrd~=2.0.1',
        'retry~=0.9.2',
        'pandas~=1.2.4',
        'beautifulsoup4~=4.9.3',
        'loguru~=0.5.3',
        'sqlalchemy~=1.4.15',
        'pyodbc~=4.0.30',
        'urllib3~=1.26.4',
        'arcgis~=1.8.5',
    ],
)
