from setuptools import setup, find_packages

setup(
    name="atves",
    version="0.1",
    author="Brian Seel",
    author_email="brian.seel@baltimorecity.gov",
    description="Wrapper around the data sources used by the automated enforcement program in Baltimore",
    packages=find_packages('src'),
    package_data={'atves': ['py.typed'], },
    python_requires='>=3.7',  # rely on dictionaries to be ordered
    package_dir={'': 'src'},
    install_requires=[
        'requests~=2.28.1',
        'xlrd~=2.0.1',
        'retry~=0.9.2',
        'pandas~=1.5.0',
        'beautifulsoup4~=4.11.1',
        'loguru~=0.6.0',
        'sqlalchemy~=1.4.41',
        'urllib3~=1.26.12',
        'arcgis~=2.0.1',
        'mechanize~=0.4.8',
        'tenacity~=8.1.0',
        'openpyxl~=3.0.10',
        'databasebaseclass~=0.1.1',
        'python-ntlm @ git+http://github.com/cylussec/python-ntlm@2a4e2c81e1befafe984c57a03e8a21a8de522661#egg=python-ntlm',
    ],
)
