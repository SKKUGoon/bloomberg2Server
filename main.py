from dbms.DBmssql import MSSQL

from xbbg import blp

from typing import Iterable, List, Tuple
from datetime import datetime, timedelta
import json
import time


def get_token(target:str) -> str:
    """
    Keys are saved in separate json file
        for extra security.
    :param target: type in the name for certain api
    :return: api key in str
    """
    loc = "./security/idpw.json"  # refract this file to add subtract info.
    with open(loc, 'r') as file:
        dat = json.load(file)
    file.close()

    return dat['sql'][target]


class BbgInsert:
    tdy = datetime.today()
    yst = tdy - timedelta(days=1)
    kor_tag = 'KS Equity'

    def __init__(self, rob=False, robbing=1):
        self.server = MSSQL.instance()
        self.server.login(
            id=get_token('id'),
            pw=get_token('pw')
        )
        # Monday Check
        if rob is True:
            self.tdy = datetime.today() - timedelta(days=robbing)
            self.yst = self.tdy - timedelta(days=1)
            print(f"Robbing date set to {self.tdy}")

        self.__date_excp_chk(self.tdy)

    def __date_excp_chk(self, date:datetime) -> None:
        if date.strftime('%w') == 0:  # Yesterday is Sunday
            self.yst = self.tdy - timedelta(days=3)  # Set as Friday

    def create_table(self) -> None:
        tnls = self.server.get_tablename('WSOL')
        tn = 'bbg'
        if tn in tnls.name.tolist():
            print("Table Already Exsits. Delete the original if necessary")
            return
        var = {
            'date': 'VARCHAR(20) Not Null',
            'stk_no': 'VARCHAR(6) Not Null',
            'bbg_no': 'Varchar(20)',
            'typ': 'Varchar(20) Not Null',
            'val': 'float'
        }

        self.server.create_table(
            table_name=tn,
            variables=var,
            database='WSOL'
        )
        self.server.create_pkey(
            table_name=tn,
            schema='dbo',
            database='WSOL',
            primary_key = ['date', 'stk_no', 'typ']
        )

    def set_data(self) -> List:
        d = datetime.today() - timedelta(days=7)
        col = ['stk_no']
        # Get KOSPI 200
        cond = f"year = {d.year} and chg_no = {d.month} and ind_='ks200'"
        r = self.server.select_db(
            database='WSOL',
            schema='dbo',
            table='indcomp',
            column=col,
            condition=cond
        )
        r = sum(r, ())

        r = [f"{stk} {self.kor_tag}" for stk in r]
        return r

    def req_bdh(self, company:Iterable, flds:str) -> [Tuple]:
        print(f"[BDH] >>> Requesting {flds} for {len(company)} Amount of Individual Companies...")
        df = blp.bdh(
            company,
            [flds],
            start_date=self.yst,
            end_date=self.yst
        )
        dnp = df.to_numpy().flatten()
        comp = [_[0] for _ in df.columns]
        print(f"[BBG] >>> Request Completed")
        prem = list()

        for stk, val in zip(comp, dnp):
            row = (
                self.tdy.strftime('%Y%m%d'),  # date
                stk[:6],  # stock number in KRX
                stk,  # stock number in BloombergTerm
                flds,  # Typ,
                val  # Value
            )
            prem.append(row)
        return prem

    def req_bdp(self, company:Iterable, flds:str) -> [Tuple]:
        print(f"[BDP] >>> Requesting {flds} for {len(company)} Amount of Individual Companies...")
        df = blp.bdp(company, [flds])
        dnp = df.to_numpy().flatten()
        print(f"[BBG] >>> Request Completed")
        prem = list()
        comp = df.index
        for stk, val in zip(comp, dnp):
            row = (
                self.tdy.strftime('%Y%m%d'),
                stk[:6],
                stk,
                flds,
                val
            )
            prem.append(row)
        return prem

    def ins_data(self, new_data:Iterable) -> None:
        insert_ = ['date', 'stk_no', 'bbg_no', 'typ', 'val']
        self.server.insert_row(
            table_name='bbg',
            schema='dbo',
            database='WSOL',
            col_=insert_,
            rows_=[new_data]
        )

    def main(self):
        kos200 = self.set_data()
        # Insert PER Estimate
        pe = self.req_bdh(kos200, 'BEST_PE_RATIO')
        for r_pe in pe:
            self.ins_data(r_pe)
        time.sleep(3)

        # Insert PBR  Estimate
        pb = self.req_bdh(kos200, 'BEST_PX_BPS_RATIO')
        for r_pb in pb:
            self.ins_data(r_pb)
        time.sleep(3)

        # Insert EPS  Estimate
        ep = self.req_bdp(kos200, 'BEST_EPS')
        for r_ep in ep:
            self.ins_data(r_ep)
        time.sleep(3)

        # Insert PER
        cpe = self.req_bdh(kos200, 'PE_RATIO')
        for r_cpe in cpe:
            self.ins_data(r_cpe)
        time.sleep(3)

        # Insert PBR
        cpb = self.req_bdh(kos200, 'PX_TO_BOOK_RATIO')
        for r_cpb in cpb:
            self.ins_data(r_cpb)
        time.sleep(3)

        # Insert EPS
        cep = self.req_bdp(kos200, 'IS_EPS')
        for r_cep in cep:
            self.ins_data(r_cep)


if __name__ == '__main__':
    bbg = BbgInsert()
    bbg.create_table()
    bbg.main()

    # Empty Bloomberg

#    rob_past = 17
#    while True:
#        bbgrob = BbgInsert(rob=True, robbing=rob_past)
#
#        # Check if today is the last day of the month
#        rob_date = datetime(2021, 12, 31).strftime('%Y%m%d')
#        tdy = datetime.now().strftime('%Y%m%d')
#        if tdy != rob_date:
#            break
#
#        print("Last Day of the month. Empty out Bloomberg data.")
#        print(
#            f"Robbing {bbgrob.yst} data."
#        )
#        try:
#            bbgrob.main()
#        except Exception as e:
#            print(e)
#        rob_past += 1
#        time.sleep(1)


