"""
Define Global Settings
"""

DB_CONNECTION_STRING = '/Users/alex/Desktop/DB/wrds.duckdb'  # the directory to the sql database

DB_ADJUSTOR_FIELDS = {
    'cstat.security_daily': {
        'adjustor': 'ajexdi',
        'fields_to_adjust': ['prccd', 'prcod', 'prchd', 'prcld', 'eps'],
        'operation': '/'
    },
    'crsp.security_daily': {
        'adjustor': 'cfacpr ',
        'fields_to_adjust': ['prc', 'openprc', 'askhi', 'bidlo', 'bid', 'ask'],
        'operation': '/'
    }
}
