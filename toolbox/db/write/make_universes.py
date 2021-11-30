import logging

import pandas as pd
import pandas_market_calendars as mcal

from toolbox.db.api.sql_connection import SQLConnection

logging.basicConfig(format='%(message)s ::: %(asctime)s', datefmt='%I:%M:%S %p', level=logging.INFO)


def compustat_us_universe(max_rank: int, min_rank: int = 1, start_date: str = '2000',
                          set_indexes=True) -> None:
    """
    generates US daily indexes for compustat daily security file
    only will use the primary share for a company
    will generate a table called universe.US_min_rank_max_rank, ex US_0_3000
    :param max_rank: the max market cap rank for a company to be in the universe
    :param min_rank: the min market cap rank for a company in the universe
    :param start_date: the minimum date for creating the universe
    :param set_indexes: Should we index the universe by
    :return: None
    """
    # getting the trading calendar so we dont have bad dates
    trading_cal = mcal.get_calendar(
        'NYSE').valid_days(start_date=start_date, end_date=pd.to_datetime('today')).to_series().to_frame('trading_days')

    table_name = f'universe.CSTAT_US{"" if min_rank == 1 else "_" + str(min_rank)}_{max_rank}'

    logging.info(f'Creating table {table_name}')
    sql_ensure_schema_open = f'CREATE SCHEMA IF NOT EXISTS universe;'
    sql_ensure_table_open = f'DROP TABLE IF EXISTS {table_name};'

    sql_make_universe_table = f""" 
        CREATE TABLE {table_name} 
        AS
            SELECT date, gvkey, iid, id, ttm_min_prccd, ttm_mc, ttm_mc_rank
            FROM
                (
                SELECT date, gvkey, iid, id, ttm_min_prccd, ttm_mc, 
                    row_number() OVER (PARTITION BY (date) ORDER BY ttm_mc desc) AS ttm_mc_rank
                FROM
                (
                    SELECT * 
                    FROM
                        (
                        SELECT date, gvkey, iid, id, 
                            AVG(ABS(prccd) * cshoc) OVER (
                            PARTITION BY id ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_mc,
                            MIN(ABS(prccd)) OVER (
                            PARTITION BY id ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_min_prccd
                        FROM 
                            (
                            SELECT date, gvkey, iid, id, priusa, fic, tpci, curcdd,
                                lag(prccd, 1, NULL) OVER lagDays AS prccd, 
                                lag(cshoc, 1, NULL) OVER lagDays AS cshoc
                            FROM cstat.security_daily AS sd RIGHT JOIN trading_cal cal ON sd.date = cal.trading_days 
                            WINDOW lagDays AS (PARTITION BY id ORDER BY date) 
                            )
                        WHERE date > '{start_date}' AND
                            fic = 'USA' AND
                            tpci = '0' AND
                            curcdd = 'USD' AND
                            priusa = (CASE WHEN regexp_full_match(iid, '^[0-9]*$') THEN CAST(iid AS INTEGER) end)
                        )
                    WHERE ttm_mc > 0 AND
                          ttm_min_prccd > 3
                    )
                )
            WHERE ttm_mc_rank >= {min_rank} AND 
                ttm_mc_rank <= {max_rank};
        """
    # making the db connection
    con = SQLConnection(read_only=False).con

    con.execute(sql_ensure_schema_open)
    con.execute(sql_ensure_table_open)
    con.execute(sql_make_universe_table)
    if set_indexes:
        con.execute(f'CREATE INDEX {table_name.replace("universe.", "")}_date_idx ON {table_name} ("date")')
        con.execute(f'CREATE INDEX {table_name.replace("universe.", "")}_gvkey_idx ON {table_name} ("id")')
    con.close()

    logging.info(f'Finished Creating {table_name}')


def crsp_us_universe(max_rank: int, min_rank: int = 1, start_date: str = '1980',
                     set_indexes=True) -> None:
    """
    Generates a universe of the top N stocks domiciled in the US by market cap
    Will only use companies primary share
    :param max_rank: the max market cap rank for a company to be in the universe
    :param min_rank: the min market cap rank for a company in the universe
    :param start_date: the minimum date for creating the universe
    :param set_indexes: Should we index the universe by
    :return: None
    """
    # getting the trading calendar so we dont have bad dates
    trading_cal = mcal.get_calendar(
        'NYSE').valid_days(start_date=start_date, end_date=pd.to_datetime('today')).to_series().to_frame('trading_days')

    table_name = f'universe.CRSP_US{"" if min_rank == 1 else "_" + str(min_rank)}_{max_rank}'

    logging.info(f'Creating table {table_name}')
    sql_ensure_schema_open = f'CREATE SCHEMA IF NOT EXISTS universe;'
    sql_ensure_table_open = f'DROP TABLE IF EXISTS {table_name};'

    # need to shift data one day back so no look ahead bias in uni
    sql_make_universe_table = f""" 
    CREATE TABLE {table_name} 
    AS
        SELECT date, permno, permco, ttm_min_prc, ttm_mc, ttm_mc_rank
        FROM
            (
            SELECT date, permno, permco, ttm_min_prc, ttm_mc, 
                row_number() OVER (PARTITION BY (date) ORDER BY ttm_mc desc) AS ttm_mc_rank
            FROM
                (
                SELECT date, permno, permco, ttm_min_prc, ttm_mc
                FROM
                    (
                    SELECT date, permno, permco, shrcd,
                        AVG(ABS(prc) * shrout) OVER (
                        PARTITION BY permno ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_mc,
                        MIN(ABS(prc)) OVER (
                        PARTITION BY permno ORDER BY date ROWS BETWEEN 252 PRECEDING AND CURRENT ROW) AS ttm_min_prc
                    FROM 
                        (
                        SELECT date, permno, permco, shrcd,
                        lag(prc, 1, NULL) OVER lagDays AS prc, 
                        lag(shrout, 1, NULL) OVER lagDays AS shrout
                        FROM
                            (
                            SELECT distinct date, permno, permco, shrcd, prc, shrout
                            FROM crsp.security_daily as sd RIGHT JOIN trading_cal cal on sd.date = cal.trading_days
                            )  
                        WINDOW lagDays AS (
                            PARTITION BY permno
                            ORDER BY date
                        )   
                        )
                    WHERE date > '{start_date}' AND
                          shrcd = 11
                    )
                WHERE ttm_mc IS NOT NULL AND
                      ttm_min_prc > 3 
                )
            )
        WHERE ttm_mc_rank >= {min_rank} AND 
            ttm_mc_rank <= {max_rank}
        """

    # making the db connection
    con = SQLConnection(read_only=False).con

    con.execute(sql_ensure_schema_open)
    con.execute(sql_ensure_table_open)
    con.execute(sql_make_universe_table)
    if set_indexes:
        con.execute(f'CREATE INDEX {table_name.replace("universe.", "")}_date_idx ON {table_name} ("date")')
        con.execute(f'CREATE INDEX {table_name.replace("universe.", "")}_permno_idx ON {table_name} ("permno")')
    con.close()

    logging.info(f'Finished Creating {table_name}')
