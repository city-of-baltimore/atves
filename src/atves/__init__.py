""" atves module """
from . import atves_database, atves_schema, axsis, axsis_types, conduent, creds, financial

# Camera type
ALLCAMS = 0
REDLIGHT = 1
OVERHEIGHT = 2
SPEED = 3

__all__ = ['atves_database', 'atves_schema', 'axsis', 'axsis_types', 'conduent', 'creds', 'financial',
           'ALLCAMS', 'REDLIGHT', 'OVERHEIGHT', 'SPEED']
