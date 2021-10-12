import unittest

from pandas import (
    Timestamp,
    DataFrame,
    concat,
    MultiIndex
)

from toolbox.constitutes.constitute_adjustment import ConstituteAdjustment


class ConstituteAdjustmentTest(unittest.TestCase):

    def examples(self):
        self.foo_constitutes = DataFrame(data=[
            # symbol    entered     exited
            ['BOB', '20090101', '20120101'],  # whole thing
            ['LARY', '20100105', '20100107'],  # added and then exited
            ['JEFF', '20110302', '20200302']],  # added too late
            columns=['symbol', 'from', 'thru']
        )

        self.ca = ConstituteAdjustment()
        self.ca.add_index_info(start_date=Timestamp(year=2010, month=1, day=4, tz='UTC'),
                               end_date=Timestamp(year=2010, month=1, day=12, tz='UTC'),
                               index_constitutes=self.foo_constitutes, date_format='%Y%m%d')

        self.foo_data = DataFrame(
            data=[['BOB', '2010-01-04', 50],
                  ['BOB', '2010-01-05', 51],
                  ['BOB', '2010-01-06', 52],
                  ['BOB', '2010-01-07', 53],
                  # ['BOB', '2010-01-08', 54], this will be missing data
                  ['BOB', '2010-01-11', 55],
                  ['BOB', '2010-01-12', 56],
                  ['LARY', '2010-01-04', 20],  # should not be included
                  ['LARY', '2010-01-05', 21],
                  ['LARY', '2010-01-06', 22],
                  ['LARY', '2010-01-07', 23],
                  ['LARY', '2010-01-08', 24],  # should not be included
                  ['LARY', '2010-01-11', 25],  # should not be included
                  ['LARY', '2010-01-12', 26],  # should not be included
                  ['LARY', '2010-01-13', 27],  # should not be included
                  ['FOO', '2010-01-08', 0]],  # should be ignored
            columns=['symbol', 'date', 'factor'])

        self.adjusted_foo = DataFrame(
            data=[['BOB', Timestamp('2010-01-04', tz='UTC'), 50],
                  ['BOB', Timestamp('2010-01-05', tz='UTC'), 51],
                  ['BOB', Timestamp('2010-01-06', tz='UTC'), 52],
                  ['BOB', Timestamp('2010-01-07', tz='UTC'), 53],
                  ['BOB', Timestamp('2010-01-08', tz='UTC'), None],
                  ['BOB', Timestamp('2010-01-11', tz='UTC'), 55],
                  ['BOB', Timestamp('2010-01-12', tz='UTC'), 56],
                  ['LARY', Timestamp('2010-01-05', tz='UTC'), 21],
                  ['LARY', Timestamp('2010-01-06', tz='UTC'), 22],
                  ['LARY', Timestamp('2010-01-07', tz='UTC'), 23]],
            columns=['symbol', 'date', 'factor']).set_index(['date', 'symbol'])

        pricing_data = DataFrame(
            data=[['LARY', Timestamp('2010-01-08', tz='UTC'), 24],
                  ['LARY', Timestamp('2010-01-11', tz='UTC'), 25],
                  ['LARY', Timestamp('2010-01-12', tz='UTC'), 26]],
            columns=['symbol', 'date', 'factor']).set_index(['date', 'symbol'])

        self.adjusted_pricing = concat([pricing_data, self.adjusted_foo]).sort_values(['symbol', 'date'])

    #
    #  ************************************  add_index_info  ************************************
    #

    def test_factor_add_index_info(self):
        """
        testing the index generation in add_index_info
        has missing data (None), data that should not be included (yet to be added, has been removed) and
        irrelevant symbols
        """
        self.examples()

        # for factors
        factor_components = [(Timestamp('2010-01-04', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-05', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-06', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-07', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-08', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-11', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-12', tz='UTC'), 'BOB'),
                             (Timestamp('2010-01-05', tz='UTC'), 'LARY'),
                             (Timestamp('2010-01-06', tz='UTC'), 'LARY'),
                             (Timestamp('2010-01-07', tz='UTC'), 'LARY')]

        self.assertTrue(MultiIndex.from_tuples(factor_components).equals(self.ca.factor_components))

        # for pricing
        pricing_components = factor_components + [(Timestamp('2010-01-08', tz='UTC'), 'LARY'),
                                                  (Timestamp('2010-01-11', tz='UTC'), 'LARY'),
                                                  (Timestamp('2010-01-12', tz='UTC'), 'LARY')]
        self.assertTrue(MultiIndex.from_tuples(pricing_components).equals(self.ca.pricing_components))

    def test_throw_column_error(self):
        """
        ensuring a error will be thrown when the correct columns are not supplied
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.add_index_info(start_date=Timestamp(year=2010, month=1, day=4),
                                   end_date=Timestamp(year=2010, month=1, day=12),
                                   date_format='%Y%m%d',
                                   index_constitutes=DataFrame(columns=['foo', 'foo1', 'foo2']))
        self.assertEqual('Required column "symbol" is not present', str(em.exception))

    def test_duplicate_symbols(self):
        """
        Ensuring that passing a df with duplicate symbols will raise a ValueError
        """
        self.examples()

        self.foo_constitutes.iat[1, 0] = 'BOB'

        with self.assertRaises(ValueError) as em:
            self.ca.add_index_info(start_date=Timestamp(year=2010, month=1, day=4),
                                   end_date=Timestamp(year=2010, month=1, day=12),
                                   date_format='%Y%m%d',
                                   index_constitutes=self.foo_constitutes)
        self.assertEqual('The column symbols is 0.333 duplicates, 1 rows\n', str(em.exception))

    #
    #  ************************************  adjust_data_for_membership  ************************************
    #

    def test_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership return the correct data frame
        data given has good data to index, not seen bad tickers, and tickers with dates out of bounds
        """
        self.examples()
        filtered = self.ca.adjust_data_for_membership(data=self.foo_data, date_format='%Y-%m-%d',
                                                      contents='factor')
        self.assertTrue(self.adjusted_foo.sort_index().equals(filtered.sort_index()))

    def test_pad_adjust_data_for_membership(self):
        """
        ensuring that the pricing works correctly for adjust_data_for_membership
        """
        self.examples()
        filtered = self.ca.adjust_data_for_membership(data=self.foo_data, date_format='%Y-%m-%d',
                                                      contents='pricing')

        self.assertTrue(self.adjusted_pricing.sort_index().equals(filtered.sort_index()))

    def test_throw_error_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership throws error when not given symbols or date
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.adjust_data_for_membership(data=DataFrame(columns=['foo', 'notSymbol', 'factor']),
                                               contents='factor')
        self.assertEqual('Required column "date" is not present', str(em.exception))

    def test_no_index_set_adjust_data_for_membership(self):
        """
        ensuring adjust_data_for_membership throws error when there is no index set
        AKA add_index_info was never called
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            ConstituteAdjustment().adjust_data_for_membership(data=self.foo_data, date_format='%Y-%m-%d',
                                                              contents='factor')
        self.assertEqual('Index constitutes are not set', str(em.exception))


if __name__ == '__main__':
    unittest.main()
