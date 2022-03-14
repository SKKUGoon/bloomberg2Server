from dbms.DBmssql import MSSQL

from xbbg import blp
import pandas as pd

from typing import Set, List

class GICS:
    """
    Store 5 years worth of
    """
    def __init__(self):
        self.server = MSSQL.instance()
        self.server.login(id='wsol2', pw='wsol2')

        self.TAG = ' KS EQUITY'
        self.FLDS = 'GICS_SECTOR_NAME'

    def add_tag(self, stk:set) -> List:
        return [f"{s}{self.TAG}" for s in stk]

    def get_kospi(self, tgt_y_period:int=2015) -> Set:
        y_start = tgt_y_period
        col = ['year', 'chg_no', 'code', 'stk_no', 'ind_']
        result = list()
        for y in range(y_start, 2022 + 1):
            for m in range(1, 12 + 1):
                r = self.server.select_db(
                    database='WSOL',
                    schema='dbo',
                    table='indcomp',
                    column=col,
                    condition=f"year={y} and chg_no={m} and ind_='kospi'"
                )
                r = [stk[3] for stk in r]
                result.append(r)
        result = sum(result, [])
        return set(result)

    def get_gics(self, stks:set) -> pd.DataFrame:
        company = self.add_tag(stks)
        df = blp.bdp(company, [self.FLDS])
        df['stk'] = [name[:6] for name in df.index]
        df['std'] = ['gics'] * len(df)
        df = df[['stk', 'gics_sector_name', 'std']]
        return df

    def run(self):
        print("[BBG GICS] >>> Getting 5 years of KOSPI")
        r = self.get_kospi()
        print("[BBG GICS] >>> Requesting GICS of KOSPI stocks")
        d = self.get_gics(r)
        print("[BBG GICS] >>> Inserting Data")
        dnp = [tuple(_) for _ in d.to_numpy()]
        self.server.insert_row(
            table_name='bbg_gics',
            schema='dbo',
            database='WSOL',
            col_=['stk_no', 'cls', 'standard'],
            rows_=dnp
        )


if __name__ == "__main__":
    gics = GICS()
    gics.run()
