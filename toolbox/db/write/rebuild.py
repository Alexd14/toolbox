from toolbox.db.read.universe import clear_built_universes, clear_etf_universes
from toolbox.db.write.create_tables import IngestDataBase
from toolbox.db.write.make_universes import clear_master_ranking_table, compustat_us_universe, crsp_us_universe


def rebuild_db(drop: bool = False):
    """
    code to rebuild the database from scratch
    :param drop: should we drop the current tables
    """
    tbls = [

        # Linking

        # {
        #     'table': 'crsp_cstat_link',
        #     'schema': 'link',
        #     'file_path': '/Users/alex/Desktop/WRDS/Linking Tables/Compustat CRSP Link 20230430.gz',
        #     'custom': """
        #                 UPDATE link.crsp_cstat_link SET LINKENDDT='2050-12-31' WHERE LINKENDDT = 'E';
        #                 ALTER TABLE link.crsp_cstat_link ADD COLUMN id VARCHAR;
        #                 ALTER TABLE link.crsp_cstat_link ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #                 ALTER TABLE link.crsp_cstat_link ALTER id SET DATA TYPE VARCHAR USING CONCAT(gvkey, '_', liid);
        #                 ALTER TABLE link.crsp_cstat_link ALTER priusa SET DATA TYPE VARCHAR USING lpad(priusa, 2, '0');
        #               """,
        #     'alter_type': {'LINKDT': ['timestamp', '%Y-%m-%d'],
        #                    'LINKENDDT': ['timestamp', '%Y-%m-%d'],
        #                    'dldte': ['timestamp', '%Y-%m-%d']},
        #     'index': [{'name': 'ccm_link_lpermno_idx', 'column': 'lpermno'},
        #               {'name': 'ccm_link_gvkey_idx', 'column': 'gvkey'},
        #               {'name': 'ccm_link_liid_idx', 'column': 'liid'}]
        # },
        #
        # {
        #     'rows_to_interpret': 100,
        #     'schema': 'link',
        #     'table': 'crsp_ibes_link',
        #     'file_path': '/Users/alex/Desktop/WRDS/Linking Tables/IBES CRSP Link 20230430.gz',
        #     'alter_type': {'sdate': ['timestamp', '%Y-%m-%d'],
        #                    'edate': ['timestamp', '%Y-%m-%d']},
        #     'index':
        #         [{'name': 'crsp_ibes_permno_idx', 'column': 'permno'},
        #          {'name': 'crsp_ibes_ticker_idx', 'column': 'ticker'},
        #          {'name': 'crsp_ibes_sdate_idx', 'column': 'sdate'},
        #          {'name': 'crsp_ibes_edate_idx', 'column': 'edate'}]
        # },
        #
        # #
        # # CRSP
        # #
        # {
        #     'rows_to_interpret': 2_000_000,
        #     'table': 'sd',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Security Files/Daily Stock File/Daily Stock File 29251231-20221231.gz',
        #     'rename': {},
        #     'alter_type': {'date': ['timestamp', '%Y-%m-%d'],
        #                    'nameendt': ['timestamp', '%Y-%m-%d'],
        #                    'shrenddt': ['timestamp', '%Y-%m-%d'],
        #                    'nextdt': ['timestamp', '%Y-%m-%d'],
        #                    'dlpdt': ['timestamp', '%Y-%m-%d'],
        #                    'dclrdt': ['timestamp', '%Y-%m-%d'],
        #                    'rcrddt': ['timestamp', '%Y-%m-%d'],
        #                    'paydt': ['timestamp', '%Y-%m-%d']},
        #     'index': [{'name': 'crsp_sd_date_idx', 'column': 'date'},
        #               {'name': 'crsp_sd_permno_idx', 'column': 'permno'}]
        # },
        #
        # {
        #     'rows_to_interpret': 2_000_000,
        #     'table': 'sm',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Security Files/Monthly Stock File/Security Monthly File 192501-202203.csv.gz',
        #     'rename': {},
        #     'alter_type': {'date': ['timestamp', '%Y%m%d'],
        #                    'nameendt': ['timestamp', '%Y%m%d'],
        #                    'shrenddt': ['timestamp', '%Y%m%d'],
        #                    'nextdt': ['timestamp', '%Y%m%d'],
        #                    'dlpdt': ['timestamp', '%Y%m%d'],
        #                    'dclrdt': ['timestamp', '%Y%m%d'],
        #                    'rcrddt': ['timestamp', '%Y%m%d'],
        #                    'paydt': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'crsp_sm_date_idx', 'column': 'date'},
        #               {'name': 'crsp_sm_permno_idx', 'column': 'permno'}]
        # },
        #
        # {
        #     'table': 'stock_header_info',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Security Files/Stock Header Info/Stock Header Info 20220331.csv.gz',
        #     'custom': """
        #                 UPDATE crsp.stock_header_info SET begvol=NULL WHERE begvol = 'Z';
        #                 UPDATE crsp.stock_header_info SET endvol=NULL WHERE endvol = 'Z';
        #                """,
        #     'alter_type': {'begdat': ['timestamp', '%Y%m%d'],
        #                    'enddat': ['timestamp', '%Y%m%d'],
        #                    'begprc': ['timestamp', '%Y%m%d'],
        #                    'endprc': ['timestamp', '%Y%m%d'],
        #                    'begvol': ['timestamp', '%Y%m%d'],
        #                    'endvol': ['timestamp', '%Y%m%d'],
        #                    'begret': ['timestamp', '%Y%m%d'],
        #                    'endret': ['timestamp', '%Y%m%d']},
        #     'rename': {},
        #     'index': [{'name': 'crsp_sh_permno_idx', 'column': 'permno'}]
        # },
        # {
        #     'table': 'distributions',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Events/Distribution/Distribution 292512-202112.gz',
        #     'alter_type': {'dclrdt': ['timestamp', '%Y%m%d'],
        #                    'rcrddt': ['timestamp', '%Y%m%d'],
        #                    'paydt': ['timestamp', '%Y%m%d'],
        #                    'exdt': ['timestamp', '%Y%m%d']},
        #     'rename': {},
        #     'index': [{'name': 'crsp_dist_permno_idx', 'column': 'permno'}]
        # },
        # {
        #     'rows_to_interpret': 3_000_000,
        #     'table': 'names',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Annual Update/Stock : Events/Names/Names 202112.gz',
        #     'alter_type': {'nameendt': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'crsp_name_permno_idx', 'column': 'date'},
        #               {'name': 'crsp_name_ticker_idx', 'column': 'ticker'}]
        # },
        # {
        #     'rows_to_interpret': 2_000_000,
        #     'table': 'fund_summary',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Quarterly Update/Mutual Fund/Fund Summary/Fund Summary 196912-202212 - no mng_name.gz',
        #     'rename': {'caldt': 'date'},
        #     'alter_type': {'date': ['timestamp', '%Y-%m-%d'],
        #                    'first_offer_dt': ['timestamp', '%Y%m%d'],
        #                    'mgr_dt': ['timestamp', '%Y%m%d'],
        #                    'end_dt': ['timestamp', '%Y%m%d'],
        #                    'nav_latest_dt': ['timestamp', '%Y-%m-%d'],
        #                    'nav_52w_h_dt': ['timestamp', '%Y-%m-%d'],
        #                    'nav_52w_l_dt': ['timestamp', '%Y-%m-%d'],
        #                    'unrealized_app_dt': ['timestamp', '%Y-%m-%d'],
        #                    'maturity_dt': ['timestamp', '%Y-%m-%d'],
        #                    'fiscal_yearend': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'crsp_mffs_date_idx', 'column': 'date'},
        #               {'name': 'crsp_mffs_crsp_portno_idx', 'column': 'crsp_portno'},
        #               {'name': 'crsp_mffs_ticker_idx', 'column': 'ticker'}]
        # },
        # {
        #     'rows_to_interpret': 3_000_000,
        #     'table': 'portfolio_holdings',
        #     'schema': 'crsp',
        #     'file_path': '/Users/alex/Desktop/WRDS/CRSP/Quarterly Update/Mutual Fund/Portfolio Holdings/Portfolio Holdings 200101-202212.gz',
        #     'rename': {'report_dt': 'date'},
        #     'alter_type': {'date': ['timestamp', '%Y-%m-%d'],
        #                    'eff_dt': ['timestamp', '%Y-%m-%d'],
        #                    'maturity_dt': ['timestamp', '%Y-%m-%d']},
        #     'index': [{'name': 'crsp_mfph_date_idx', 'column': 'date'},
        #               {'name': 'crsp_mfph_crsp_portno_idx', 'column': 'crsp_portno'},
        #               {'name': 'crsp_mfph_permno_idx', 'column': 'permno'}]
        # },
        #
        # #
        # # Compustat
        # #
        # {
        #     'table': 'funda',
        #     'schema': 'cstat',
        #     'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Fundementals Annual/Fundementals Annual 195006-202301.gz',
        #     'custom': """
        #                 ALTER TABLE cstat.funda ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #               """,
        #     'rename': {'datadate': 'date'},
        #     'alter_type': {'date': ['timestamp', '%Y%m%d'],
        #                    'DLDTE': ['timestamp', '%Y%m%d'],
        #                    'IPODATE ': ['timestamp', '%Y%m%d'],
        #                    'APDEDATE': ['timestamp', '%Y%m%d'],
        #                    'FDATE': ['timestamp', '%Y%m%d'],
        #                    'PDATE': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'cstat_fa_date_idx', 'column': 'date'},
        #               {'name': 'cstat_fa_gvkey_idx', 'column': 'gvkey'}]
        # },
        {
            'table': 'fundq',
            'schema': 'cstat',
            'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Fundemental Quartely/Fundementals Quartely 196301-202301.gz',
            'custom': """
                      ALTER TABLE cstat.fundq ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
                      """,
            'rename': {'datadate': 'date'},
            'alter_type': {'date': ['timestamp', '%Y-%m-%d'],
                           'DLDTE': ['timestamp', '%Y-%m-%d'],
                           'IPODATE ': ['timestamp', '%Y%m%d'],
                           'APDEDATEQ': ['timestamp', '%Y%m%d'],
                           'FDATEQ': ['timestamp', '%Y%m%d'],
                           'PDATEQ': ['timestamp', '%Y%m%d'],
                           'RDQ': ['timestamp', '%Y%m%d']},
            'index': [{'name': 'cstat_fq_date_idx', 'column': 'date'},
                      {'name': 'cstat_fq_gvkey_idx', 'column': 'gvkey'}]
        },
        # {
        #     'rows_to_interpret': 2_000_000,
        #     'table': 'sd',
        #     'schema': 'main',
        #     'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Security Daily/Security Daily 19831231-20230428csv.gz',
        #     'custom': """
        #                 ALTER TABLE main.sd ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #                 ALTER TABLE main.sd ADD COLUMN id VARCHAR;
        #                 ALTER TABLE main.sd ALTER id SET DATA TYPE VARCHAR USING CONCAT(gvkey, '_', iid);
        #                 -- ALTER TABLE main.sd DROP BUSDESC;
        #               """,
        #     'rename': {'datadate': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y-%m-%d'],
        #                    'DLDTE': ['timestamp', '%Y-%m-%d'],
        #                    'IPODATE': ['timestamp', '%Y%m%d'],
        #                    'ANNCDATE': ['timestamp', '%Y-%m-%d'],
        #                    'CAPGNPAYDATE': ['timestamp', '%Y-%m-%d'],
        #                    'CHEQVPAYDATE': ['timestamp', '%Y-%m-%d'],
        #                    'DIVDPAYDATE': ['timestamp', '%Y-%m-%d'],
        #                    'DIVSPPAYDATE': ['timestamp', '%Y-%m-%d'],
        #                    'PAYDATE': ['timestamp', '%Y-%m-%d'],
        #                    'RECORDDATE': ['timestamp', '%Y-%m-%d']
        #                    },
        #     'index': [{'name': 'cstat_sd_date_idx', 'column': 'date'},
        #               {'name': 'cstat_sd_gvkey_idx', 'column': 'gvkey'},
        #               {'name': 'cstat_sd_iid_idx', 'column': 'iid'},
        #               {'name': 'cstat_sd_id_idx', 'column': 'id'}]
        # },
        # {
        #     'rows_to_interpret': 50_000,
        #     'table': 'sm',
        #     'schema': 'cstat',
        #     'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Security Monthly/Security Monthly 196201-202301.csv.gz',
        #     'custom': """
        #                     ALTER TABLE cstat.sm ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #                     ALTER TABLE cstat.sm ADD COLUMN id VARCHAR;
        #                     ALTER TABLE cstat.sm ALTER id SET DATA TYPE VARCHAR USING CONCAT(gvkey, '_', iid);
        #                   """,
        #     'rename': {'datadate': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
        #                    'DLDTE': ['timestamp', '%Y%m%d'],
        #                    'IPODATE': ['timestamp', '%Y%m%d'],
        #                    },
        #     'index': [{'name': 'cstat_sm_date_idx', 'column': 'date'},
        #               {'name': 'cstat_sm_gvkey_idx', 'column': 'gvkey'},
        #               {'name': 'cstat_sm_iid_idx', 'column': 'iid'},
        #               {'name': 'cstat_sm_id_idx', 'column': 'id'}]
        # },
        #
        # {
        #     'rows_to_interpret': 200_000,
        #     'table': 'short_interest',
        #     'schema': 'cstat',
        #     'file_path': '/Users/alex/Desktop/WRDS/Compustat - Capital IQ/Compustat/North America/Supplemental Short Intrest File/Supplemental Short Intrest File 197301-202205.gz',
        #     'custom': """
        #                 -- id is being read as int so have to make varchar and pad
        #                 ALTER TABLE cstat.short_interest ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #                 ALTER TABLE cstat.short_interest ALTER iid SET DATA TYPE VARCHAR USING
        #                     CASE WHEN iid < 10 THEN CONCAT('0', iid) ELSE iid END;
        #                 ALTER TABLE cstat.short_interest ADD COLUMN id VARCHAR;
        #                 ALTER TABLE cstat.short_interest ALTER id SET DATA TYPE VARCHAR USING CONCAT(gvkey, '_', iid);
        #                 """,
        #     'rename': {'SPLITADJDATE': 'date'},
        #     'alter_type': {'date': ['timestamp', '%Y%m%d'],
        #                    'datadate': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'cstat_si_date_idx', 'column': 'date'},
        #               {'name': 'cstat_si_gvkey_idx', 'column': 'gvkey'},
        #               {'name': 'cstat_si_iid_idx', 'column': 'iid'},
        #               {'name': 'cstat_si_id_idx', 'column': 'id'}]
        # },
        #
        #
        # WRDS
        #
        #
        # {
        #     'rows_to_interpret': 50_000,
        #     'schema': 'wrds',
        #     'table': 'firm_ratios',
        #     'file_path': '/Users/alex/Desktop/WRDS/Finical Ratio Suite by WRDS/Finanical Ratios /IBES Financial Ratios By Firm Level WRDS/Financial Ratios IBES 19700131-20220307.gz',
        #     'custom': """
        #                   ALTER TABLE wrds.firm_ratios ALTER gvkey SET DATA TYPE VARCHAR USING lpad(gvkey, 6, '0');
        #                """,
        #     'rename': {'public_date': 'date'},
        #     'alter_type': {'adate': ['timestamp', '%Y%m%d'],
        #                    'qdate': ['timestamp', '%Y%m%d'],
        #                    'date': ['timestamp', '%Y%m%d']},
        #     'index':
        #         [{'name': 'wrds_firm_date_idx', 'column': 'date'},
        #          {'name': 'wrds_firm_permno_idx', 'column': 'permno'},
        #          {'name': 'wrds_firm_gvkey_idx', 'column': 'gvkey'}]
        # },
        #
        # {
        #     'rows_to_interpret': 50_000,
        #     'schema': 'wrds',
        #     'table': 'subsidiary',
        #     'file_path': '/Users/alex/Desktop/WRDS/Subsidary Data By WRDS/Company Subsidiaries/WRDS Company Subidiary Data (Beta) 199312-202004.gz',
        #     'rename': {'SECPDATE': 'date'},
        #     'alter_type': {'FDATE': ['timestamp', '%Y%m%d'],
        #                    'RDATE': ['timestamp', '%Y%m%d'],
        #                    'date': ['timestamp', '%Y%m%d']},
        #     'index':
        #         [{'name': 'wrds_sub_date_idx', 'column': 'date'},
        #          {'name': 'wrds_sub_gvkey_idx', 'column': 'gvkey'}]
        # },
        #
        #
        # IBE
        #
        #
        # {
        #     'rows_to_interpret': 50_000,
        #     'table': 'summary_consensus',
        #     'schema': 'ibes',
        #     'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Summary Statistics/Summary Statistics 197601-202212.csv.gz',
        #     'rename': {'FPEDATS': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
        #                    'STATPERS': ['timestamp', '%Y%m%d'],
        #                    'ANNDATS_ACT': ['timestamp', '%Y%m%d'],
        #                    },
        #     'index': [{'name': 'ibes_sc_date_idx', 'column': 'date'},
        #               {'name': 'ibes_sc_ticker_idx', 'column': 'ticker'},
        #               {'name': 'ibes_sc_measure_idx', 'column': 'measure'},
        #               {'name': 'ibes_sc_fpi_idx', 'column': 'fpi'}]
        # },
        # {
        #     'rows_to_interpret': 5000,
        #     'table': 'summary_price_target',
        #     'schema': 'ibes',
        #     'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Price Target/lyrvpqbb4tg2lbv0.csv',
        #     'rename': {'STATPERS': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'ibes_spt_date_idx', 'column': 'date'},
        #               {'name': 'ibes_spt_ticker_idx', 'column': 'ticker'},
        #               {'name': 'ibes_spt_usfirm_idx', 'column': 'usfirm'},
        #               {'name': 'ibes_spt_curr_idx', 'column': 'curr'}]
        # },
        #
        # {
        #     'rows_to_interpret': 5000,
        #     'table': 'summary_price_target',
        #     'schema': 'ibes',
        #     'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Price Target/lyrvpqbb4tg2lbv0.csv',
        #     'rename': {'STATPERS': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'ibes_spt_date_idx', 'column': 'date'},
        #               {'name': 'ibes_spt_ticker_idx', 'column': 'ticker'},
        #               {'name': 'ibes_spt_usfirm_idx', 'column': 'usfirm'},
        #               {'name': 'ibes_spt_curr_idx', 'column': 'curr'}]
        # },
        # {
        #     'rows_to_interpret': 50000,
        #     'table': 'summary_statistics',
        #     'schema': 'ibes',
        #     'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Summary History/Summary Statistics/1fhrqwpqzncqbdp3.csv',
        #     'rename': {'STATPERS': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
        #                    'ANNDATS_ACT': ['timestamp', '%Y%m%d'],
        #                    'FPEDATS': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'ibes_ss_date_idx', 'column': 'date'},
        #               {'name': 'ibes_ss_ticker_idx', 'column': 'ticker'},
        #               {'name': 'ibes_ss_usfirm_idx', 'column': 'usfirm'},
        #               {'name': 'ibes_ss_currcode_idx', 'column': 'curcode'},
        #               {'name': 'ibes_ss_measure_idx', 'column': 'measure'}]
        # },
        # {
        #     'rows_to_interpret': 50000,
        #     'table': 'detail',
        #     'schema': 'ibes',
        #     'file_path': '/Users/alex/Desktop/WRDS/IBES/IBES Academic/Detail History/Detail File WIth Actuals/Detail File With Actuals 197001-202108.csv.gz',
        #     'rename': {'ACTDATS': 'date'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
        #                    'FPEDATS': ['timestamp', '%Y%m%d'],
        #                    'REVDATS': ['timestamp', '%Y%m%d'],
        #                    'ANNDATS': ['timestamp', '%Y%m%d'],
        #                    'ANNDATS_ACT': ['timestamp', '%Y%m%d'],
        #                    'ACTDATS_ACT': ['timestamp', '%Y%m%d']},
        #     'index': [{'name': 'ibes_ss_date_idx', 'column': 'date'},
        #               {'name': 'ibes_ss_ticker_idx', 'column': 'ticker'},
        #               {'name': 'ibes_ss_usfirm_idx', 'column': 'usfirm'},
        #               {'name': 'ibes_ss_measure_idx', 'column': 'measure'},
        #               {'name': 'ibes_ss_fpi_idx', 'column': 'fpi'}]
        # },
        #
        # #
        # # Audit Analytics
        # #
        # {
        #     'rows_to_interpret': 500000,
        #     'table': 'audit_opinions',
        #     'schema': 'aa',
        #     'file_path': '/Users/alex/Desktop/WRDS/Audit Analytics/Audit Opinions/lbsyljuigehonvhr.csv',
        #     'rename': {'FILE_ACCEPTED': 'date',
        #                'COMPANY_FKEY': 'cik'},
        #     'alter_type': {'DATE': ['timestamp', '%Y%m%d'],
        #                    # 'FISCAL_YEAR_ENDED_OF_OPINION': ['timestamp', '%Y%m%d']
        #                    },
        #     'index': [{'name': 'aa_ss_date_idx', 'column': 'date'},
        #               {'COMPANY_FKEY': 'aa_ss_ticker_idx', 'column': 'cik'}]
        # }
    ]

    # building the tables, chunking the tables to insert bc if we dont then them tmep will balloon in size
    for inner_tbl_list in [tbls[i:i + 3] for i in range(0, len(tbls), 3)]:
        IngestDataBase().ingest(inner_tbl_list, drop, rows_to_interpret=20000)

    # clearing the etf universe cache
    clear_etf_universes()
    # clearing the built universes
    clear_built_universes()

    # building crsp universes
    crsp_us_universe(max_rank=500, rebuild_mc_ranking=True, link=True)
    crsp_us_universe(max_rank=1000, link=True)
    crsp_us_universe(max_rank=3000, link=True)
    crsp_us_universe(min_rank=1000, max_rank=3000, link=True)

    # building compustat universes
    compustat_us_universe(max_rank=500, rebuild_mc_ranking=True)
    compustat_us_universe(max_rank=1000)
    compustat_us_universe(max_rank=3000)
    compustat_us_universe(min_rank=1000, max_rank=3000)

    # drop sql universe schema
    clear_master_ranking_table()


if __name__ == '__main__':
    rebuild_db(drop=True)
