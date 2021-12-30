"""
Define Global Settings
"""

DB_CONNECTION_STRING = '/Users/alex/Desktop/DB/wrds.duckdb'  # the directory to the sql database
CACHE_DIRECTORY = '/tmp/toolbox_cache'  # the directory to cache files

DB_ADJUSTOR_FIELDS = {
    'cstat.security_daily': [
        {
            'adjustor': 'ajexdi',
            'fields_to_adjust': ['prccd', 'prcod', 'prchd', 'prcld', 'eps'],
            'operation': '/'
        }
    ],
    'crsp.security_daily': [
        {
            'adjustor': 'cfacpr',
            'fields_to_adjust': ['prc', 'openprc', 'askhi', 'bidlo', 'bid', 'ask'],
            'operation': '/',
            'function': 'ABS'
        },
        {
            'adjustor': 'cfacshr',
            'fields_to_adjust': ['vol', 'shrout'],
            'operation': '*'
        }

    ],
    'cstat.fundamental_annual': [
        {'fields_to_adjust': []}
    ],

    'wrds.firm_ratios': [
        {'fields_to_adjust': []}
    ],
    'ibes.summary_price_target': [
        {'fields_to_adjust': []}
    ]
}
