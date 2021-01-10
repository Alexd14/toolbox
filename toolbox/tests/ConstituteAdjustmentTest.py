import unittest

from pandas import Timestamp, DataFrame

from toolbox.constitutes.ConstituteAdjustment import ConstituteAdjustment


class ConstituteAdjustmentTest(unittest.TestCase):

    def examples(self):
        self.fooConstitutes = DataFrame(data=[
            # symbol    entered     exited
            ['BOB', '20090101', '20120101'],  # whole thing
            ['LARY', '20100105', '20100107'],  # added and then exited
            ['JEFF', '20110302', '20200302']],  # added too late
            columns=['symbol', 'from', 'thru']
        )

        self.ca = ConstituteAdjustment()
        self.ca.addIndexInfo(startingDate=Timestamp(year=2010, month=1, day=4),
                             endingDate=Timestamp(year=2010, month=1, day=12),
                             indexConstitutes=self.fooConstitutes, dateFormat='%Y%m%d',
                             pad=10)

        self.fooData = DataFrame(
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
                  ['LARY', '2011-07-01', 24],  # should not be included
                  ['FOO', '2010-01-08', 0]],  # should be ignored
            columns=['symbol', 'date', 'factor'])

        self.adjustedFoo = DataFrame(
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

    #
    #  ************************************  addIndexInfo  ************************************
    #

    def test_addIndexInfo(self):
        """
        testing the index generation in addIndexInfo
        has missing data (None), data that should not be included (yet to be added, has been removed) and
        irrelevant symbols
        """
        self.examples()

        components = [(Timestamp('2010-01-04', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-05', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-06', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-07', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-08', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-11', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-12', tz='UTC'), 'BOB'),
                      (Timestamp('2010-01-05', tz='UTC'), 'LARY'),
                      (Timestamp('2010-01-06', tz='UTC'), 'LARY'),
                      (Timestamp('2010-01-07', tz='UTC'), 'LARY')]
        self.assertEqual(components, self.ca.components)

    def test_throwColumnError(self):
        """
        ensuring a error will be thrown when the correct columns are not supplied
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.addIndexInfo(startingDate=Timestamp(year=2010, month=1, day=4),
                                 endingDate=Timestamp(year=2010, month=1, day=12),
                                 dateFormat='%Y%m%d',
                                 indexConstitutes=DataFrame(columns=['foo', 'foo1', 'foo2']),
                                 pad=10)
        self.assertEqual('Required column "symbol" is not present', str(em.exception))

    def test_duplicateSymbols(self):
        """
        Ensuring that passing a df with duplicate symbols will raise a ValueError
        """
        self.examples()

        self.fooConstitutes.iat[1, 0] = 'BOB'

        with self.assertRaises(ValueError) as em:
            self.ca.addIndexInfo(startingDate=Timestamp(year=2010, month=1, day=4),
                                 endingDate=Timestamp(year=2010, month=1, day=12),
                                 dateFormat='%Y%m%d',
                                 indexConstitutes=self.fooConstitutes,
                                 pad=10)
        self.assertEqual('The column symbols is 0.333 duplicates, 1 rows\n', str(em.exception))

    #
    #  ************************************  adjustDataForMembership  ************************************
    #

    def test_adjustDataForMembership(self):
        """
        ensuring adjustDataForMembership return the correct data frame
        data given has good data to index, not seen bad tickers, and tickers with dates out of bouns
        """
        self.examples()
        filtered = self.ca.adjustDataForMembership(data=self.fooData, dateFormat='%Y-%m-%d', representation='factor')
        self.assertTrue(self.adjustedFoo.equals(filtered))

    def test_throwError_adjustDataForMembership(self):
        """
        ensuring adjustDataForMembership throws error when not given symbols or date
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            self.ca.adjustDataForMembership(data=DataFrame(columns=['foo', 'notSymbol', 'factor']),
                                            representation='factor')
        self.assertEqual('Required column "date" is not present', str(em.exception))

    def test_noIndexSet_adjustDataForMembership(self):
        """
        ensuring adjustDataForMembership throws error when there is no index set
        AKA addIndexInfo was never called
        """
        self.examples()

        with self.assertRaises(ValueError) as em:
            ConstituteAdjustment().adjustDataForMembership(data=self.fooData, dateFormat='%Y-%m-%d',
                                                           representation='factor')
        self.assertEqual('Index constitutes are not set', str(em.exception))


if __name__ == '__main__':
    unittest.main()
