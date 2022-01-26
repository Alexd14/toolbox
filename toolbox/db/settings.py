"""
Define Global Settings
"""

DB_CONNECTION_STRING = '/Users/alex/Desktop/DB/wrds.duckdb'  # the directory to the sql database
CACHE_DIRECTORY = '/tmp'  # the directory to cache files

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

# sql code to link permno to cstat and ibes
ADD_ALL_LINKS_TO_PERMNO = """
(
    SELECT --columns 
    FROM
        --from LEFT JOIN link.crsp_cstat_link AS ccm ON (uni.permno = ccm.lpermno AND uni.date >= ccm.linkdt 
                AND uni.date <= ccm.linkenddt AND (ccm.linktype = 'LU' OR ccm.linktype = 'LC'))
        LEFT JOIN link.crsp_ibes_link AS crib ON (uni.permno = crib.permno AND uni.date >= crib.sdate 
            AND uni.date <= crib.edate)
)
"""
