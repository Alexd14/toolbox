from toolbox.db.write.create_tables import IngestDataBase
from toolbox.db.write.make_universes import compustat_us_universe, crsp_us_universe


def rebuild_db(drop: bool = False):
    """
    code to rebuild the database from scratch
    :param drop: should we drop the current tables
    """
    tbls = [
        #
        # CRSP Compustat Link
        #
        {
            'table': 'crsp_cstat_link',
            'schema': 'ccm',
            'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/CRSP:Compustst Merged/Compustat CRSP Link/rc9ie3efp9e3opdf.csv',
            'custom': """
                        UPDATE ccm.crsp_cstat_link SET LINKENDDT=99991231 WHERE LINKENDDT = 'E';
                      """,
            'alter_type': {'LINKDT': ['timestamp', '%Y%m%d'],
                           'LINKENDDT': ['timestamp', '%Y%m%d'],
                           'dldte': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'ccm_link_lpermno_idx', 'column': 'lpermno'},
                      {'name': 'ccm_link_gvkey_idx', 'column': 'gvkey'},
                      {'name': 'ccm_link_liid_idx', 'column': 'liid'}]
        },

        #
        # CRSP
        #
        {
            'rows_to_interpret': 2_000_000,
            'table': 'security_daily',
            'schema': 'crsp',
            'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Security Files/Daily Stock File/ndrekzi6lud82dpo.csv',
            'rename': {},
            'alter_type': {'date': ['timestamp', '%Y%m%d'],
                           'nameendt': ['timestamp', '%Y%m%d'],
                           'shrenddt': ['timestamp', '%Y%m%d'],
                           'nextdt': ['timestamp', '%Y%m%d'],
                           'dlpdt': ['timestamp', '%Y%m%d'],
                           'dclrdt': ['timestamp', '%Y%m%d'],
                           'rcrddt': ['timestamp', '%Y%m%d'],
                           'paydt': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'crsp_sd_date_idx', 'column': 'date'},
                      {'name': 'crsp_sd_permno_idx', 'column': 'permno'}]
        },

        {
            'table': 'stock_header_info',
            'schema': 'crsp',
            'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Security Files/Stock Header Info/2xzpc1ww0dih4jk0.csv',
            'custom': """
                        UPDATE crsp.stock_header_info SET begvol=NULL WHERE begvol = 'Z';
                        UPDATE crsp.stock_header_info SET endvol=NULL WHERE endvol = 'Z';
                       """,
            'alter_type': {'begdat': ['timestamp', '%Y%m%d'],
                           'enddat': ['timestamp', '%Y%m%d'],
                           'begprc': ['timestamp', '%Y%m%d'],
                           'endprc': ['timestamp', '%Y%m%d'],
                           'begvol': ['timestamp', '%Y%m%d'],
                           'endvol': ['timestamp', '%Y%m%d'],
                           'begret': ['timestamp', '%Y%m%d'],
                           'endret': ['timestamp', '%Y%m%d']},
            'rename': {},
            'index': [{'name': 'crsp_sh_permno_idx', 'column': 'permno'}]
        },
        {
            'table': 'distributions',
            'schema': 'crsp',
            'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Events/Distribution/y0vypfj8geyzhs0l.csv',
            'alter_type': {'dclrdt': ['timestamp', '%Y%m%d'],
                           'rcrddt': ['timestamp', '%Y%m%d'],
                           'paydt': ['timestamp', '%Y%m%d'],
                           'exdt': ['timestamp', '%Y%m%d']},
            'rename': {},
            'index': [{'name': 'crsp_dist_permno_idx', 'column': 'permno'}]
        },

        #
        # Compustat
        #
        {
            'table': 'fundamental_annual',
            'schema': 'cstat',
            'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Fundementals Annual/gkm6i8iuxd46uuw1.csv',
            'rename': {'pdate': 'date'},
            'alter_type': {'date': ['timestamp', '%Y%m%d'],
                           'DLDTE': ['timestamp', '%Y%m%d'],
                           'IPODATE ': ['timestamp', '%Y%m%d'],
                           'APDEDATE': ['timestamp', '%Y%m%d'],
                           'FDATE': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'cstat_fa_date_idx', 'column': 'date'},
                      {'name': 'cstat_fa_gvkey_idx', 'column': 'gvkey'}]
        },
        {
            'rows_to_interpret': 500_000,
            'table': 'security_daily',
            'schema': 'cstat',
            'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Security Daily/3wbicrumplz4cjvu.csv',
            'custom': """
                        ALTER TABLE cstat.security_daily ADD COLUMN id VARCHAR;
                        ALTER TABLE cstat.security_daily ALTER id SET DATA TYPE VARCHAR USING CONCAT(gvkey, '_', iid);
                      """,
            'rename': {'datadate': 'date'},
            'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
                           'DLDTE': ['timestamp', '%Y%m%d'],
                           'IPODATE': ['timestamp', '%Y%m%d'],
                           'ANNCDATE': ['timestamp', '%Y%m%d'],
                           'CAPGNPAYDATE': ['timestamp', '%Y%m%d'],
                           'CHEQVPAYDATE': ['timestamp', '%Y%m%d'],
                           'DIVDPAYDATE': ['timestamp', '%Y%m%d'],
                           'DIVSPPAYDATE': ['timestamp', '%Y%m%d'],
                           'PAYDATE': ['timestamp', '%Y%m%d'],
                           'RECORDDATE': ['timestamp', '%Y%m%d']
                           },
            'index': [{'name': 'cstat_sd_date_idx', 'column': 'date'},
                      {'name': 'cstat_sd_gvkey_idx', 'column': 'gvkey'},
                      {'name': 'cstat_sd_iid_idx', 'column': 'iid'},
                      {'name': 'cstat_sd_id_idx', 'column': 'id'}]
        },

        #
        # WRDS Financial Ratios
        #

        {
            'rows_to_interpret': 50_000,
            'schema': 'wrds',
            'table': 'firm_ratios',
            'file_path': '/Users/alex/Desktop/WRDS/Finical Ratio Suite by WRDS/Finanical Ratios /IBES Financial Ratios By Firm Level WRDS/Financial Ratios IBES 19700131-20210102.gz',
            'rename': {'public_date': 'date'},
            'alter_type': {'sdate': ['timestamp', '%Y%m%d'],
                           'edate': ['timestamp', '%Y%m%d']},
            'index':
                [{'name': 'wrds_firm_date_idx', 'column': 'date'},
                 {'name': 'wrds_firm_permno_idx', 'column': 'permno'},
                 {'name': 'wrds_firm_gvkey_idx', 'column': 'gvkey'}]
        },

        #
        # IBES
        #

        {
            'rows_to_interpret': 100,
            'schema': 'ibes',
            'table': 'crsp_ibes_link',
            'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES CRSP Link/luhmjdovofexjxwg.csv',
            'alter_type': {'sdate': ['timestamp', '%Y%m%d'],
                           'edate': ['timestamp', '%Y%m%d']},
            'index':
                [{'name': 'crsp_ibes_permno_idx', 'column': 'permno'},
                 {'name': 'crsp_ibes_ticker_idx', 'column': 'ticker'},
                 {'name': 'crsp_ibes_sdate_idx', 'column': 'sdate'},
                 {'name': 'crsp_ibes_edate_idx', 'column': 'edate'}]
        },

        {
            'rows_to_interpret': 5000,
            'table': 'summary_price_target',
            'schema': 'ibes',
            'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Price Target/lyrvpqbb4tg2lbv0.csv',
            'rename': {'STATPERS': 'date'},
            'alter_type': {'DATE': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'ibes_spt_date_idx', 'column': 'date'},
                      {'name': 'ibes_spt_ticker_idx', 'column': 'ticker'},
                      {'name': 'ibes_spt_usfirm_idx', 'column': 'usfirm'},
                      {'name': 'ibes_spt_curr_idx', 'column': 'curr'}]
        },
        {
            'rows_to_interpret': 50000,
            'table': 'summary_statistics',
            'schema': 'ibes',
            'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Summary Statistics/1fhrqwpqzncqbdp3.csv',
            'rename': {'STATPERS': 'date'},
            'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
                           'ANNDATS_ACT': ['timestamp', '%Y%m%d'],
                           'FPEDATS': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'ibes_ss_date_idx', 'column': 'date'},
                      {'name': 'ibes_ss_ticker_idx', 'column': 'ticker'},
                      {'name': 'ibes_ss_usfirm_idx', 'column': 'usfirm'},
                      {'name': 'ibes_ss_currcode_idx', 'column': 'curcode'},
                      {'name': 'ibes_ss_measure_idx', 'column': 'measure'}]
        }
    ]

    # building the tables
    IngestDataBase().ingest(tbls, drop, rows_to_interpret=2000)

    # building crsp universes
    crsp_us_universe(max_rank=100)
    crsp_us_universe(max_rank=500)
    crsp_us_universe(max_rank=750)
    crsp_us_universe(max_rank=1000)
    crsp_us_universe(max_rank=3000)
    crsp_us_universe(min_rank=1000, max_rank=3000)

    # building compustat universes
    compustat_us_universe(max_rank=100)
    compustat_us_universe(max_rank=500)
    compustat_us_universe(max_rank=750)
    compustat_us_universe(max_rank=1000)
    compustat_us_universe(max_rank=3000)
    compustat_us_universe(min_rank=1000, max_rank=3000)
