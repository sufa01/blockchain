"""
Data fetching module for bonds and market data from Moscow Exchange.
Handles API requests and data parsing.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time


class BondsDataFetcher:
    """
    Fetches bonds data from Moscow Exchange (MOEX).
    """

    def __init__(self):
        self.base_url = "https://iss.moex.com/iss"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })

    def fetch_bonds_list(self,
                         asset_type: str = "stock",
                         board_group: str = "stock_bonds") -> pd.DataFrame:
        """
        Fetch list of all bonds from MOEX.
        """
        url = f"{self.base_url}/securities.json"
        params = {
            'asset_type': asset_type,
            'board_group': board_group,
            'limit': 100,
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            securities = data.get('securities', {}).get('data', [])
            columns = data.get('securities', {}).get('columns', [])
            df = pd.DataFrame(securities, columns=columns)
            return df
        except requests.exceptions.RequestException as e:
            print(f"Error fetching bonds list: {e}")
            return pd.DataFrame()

    def search_bonds_by_issuer(self,
                               issuer_name: str,
                               isin: Optional[str] = None) -> pd.DataFrame:
        """
        Search bonds by issuer name or ISIN.
        """
        url = f"{self.base_url}/securities.json"
        params = {
            'q': issuer_name,
            'asset_type': 'stock',
            'board_group': 'stock_bonds',
            'limit': 50,
        }

        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            securities = data.get('securities', {}).get('data', [])
            columns = data.get('securities', {}).get('columns', [])
            df = pd.DataFrame(securities, columns=columns)
            if issuer_name and not df.empty:
                df = df[df['name'].str.contains(issuer_name, case=False, na=False) |
                       df['shortname'].str.contains(issuer_name, case=False, na=False)]
            return df
        except requests.exceptions.RequestException as e:
            print(f"Error searching bonds by issuer: {e}")
            return pd.DataFrame()

    def get_bond_details(self, secid: str) -> Dict:
        """
        Get detailed information about a specific bond.
        """
        url = f"{self.base_url}/securities/{secid}.json"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            bond_info = {}
            if 'description' in data:
                description = data['description'].get('data', [])
                columns = data['description'].get('columns', [])
                for row in description:
                    if len(row) >= 2:
                        bond_info[row[0]] = row[1]
            return bond_info
        except requests.exceptions.RequestException as e:
            print(f"Error fetching bond details for {secid}: {e}")
            return {}

    def get_bond_market_data(self, secid: str, boardid: str = "TQCB") -> pd.DataFrame:
        """
        Get market data for a specific bond (yields, prices, etc).
        """
        url = f"{self.base_url}/engines/stock/markets/bonds/boards/{boardid}/securities/{secid}.json"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            market_data = data.get('marketdata', {}).get('data', [])
            columns = data.get('marketdata', {}).get('columns', [])
            df = pd.DataFrame(market_data, columns=columns)
            return df
        except requests.exceptions.RequestException as e:
            print(f"Error fetching market data for {secid}: {e}")
            return pd.DataFrame()

    def get_bond_ytm(self, secid: str) -> Optional[float]:
        """
        Get Yield to Maturity (YTM) for a bond.
        """
        market_data = self.get_bond_market_data(secid)
        if not market_data.empty:
            for col in ['YIELDDAY', 'YIELD', 'YIELDATBEST']:
                if col in market_data.columns:
                    ytm = market_data[col].iloc[0]
                    if pd.notna(ytm):
                        return float(ytm)
        return None

    def enrich_with_ytm(self, bonds_df: pd.DataFrame, boardid: str = "TQCB") -> pd.DataFrame:
        """
        Enrich bonds DataFrame with real YTM from MOEX API.
        """
        ytm_values = []
        for _, row in bonds_df.iterrows():
            secid = row['secid']
            ytm = self.get_bond_ytm(secid)
            if ytm is not None:
                ytm_values.append(ytm)
                print(f"  OK {secid}: YTM = {ytm:.2f}%")
            else:
                fallback = row.get('coupon_rate', 17.0)
                ytm_values.append(fallback)
                print(f"  FALLBACK {secid}: YTM not available, using coupon {fallback:.2f}%")
        bonds_df['ytm_primary'] = ytm_values
        return bonds_df

        def find_comparable_bonds(self,
                                 target_maturity_months: int = 12,
                                 placement_date: str = "2026-04",
                                 max_results: int = 10,
                                 min_volume: Optional[float] = None) -> pd.DataFrame:
            """Find bonds comparable to DFA with filtering."""
            try:
                today = datetime.now()
                min_maturity = today + timedelta(days=target_maturity_months * 30 - 60)
                max_maturity = today + timedelta(days=target_maturity_months * 30 + 60)

                # Правильный запрос к MOEX: ТОЛЬКО облигации, ТОЛЬКО TQCB
                url = f"{self.base_url}/engines/stock/markets/bonds/boards/TQCB/securities.json"
                params = {
                    'securities.columns': (
                        'SECID,SECNAME,SHORTNAME,EMITENT_TITLE,'
                        'MATDATE,ISSUEDATE,FACEVALUE,ISSUESIZE,'
                        'COUPONRATE,COUPONVALUE,COUPONPERIOD'
                    ),
                    'marketdata.columns': (
                        'SECID,YIELD,YIELDDATE,DURATION,LAST'
                    )
                }

                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Парсим securities
                securities = data.get('securities', {}).get('data', [])
                sec_columns = data.get('securities', {}).get('columns', [])

                # Парсим marketdata (YTM)
                marketdata = data.get('marketdata', {}).get('data', [])
                mkt_columns = data.get('marketdata', {}).get('columns', [])

                if not securities:
                    print("⚠️ MOEX API: нет данных по облигациям TQCB")
                    return pd.DataFrame()

                df = pd.DataFrame(securities, columns=sec_columns)
                df_mkt = pd.DataFrame(marketdata, columns=mkt_columns) if marketdata else pd.DataFrame()

                print(f"📊 MOEX API: получено {len(df)} облигаций (TQCB)")

                # Объединяем с рыночными данными (YTM)
                if not df_mkt.empty and 'SECID' in df_mkt.columns and 'YIELD' in df_mkt.columns:
                    df = df.merge(df_mkt[['SECID', 'YIELD']], on='SECID', how='left')
                else:
                    df['YIELD'] = None

                # Фильтр по дате погашения
                if 'MATDATE' in df.columns:
                    df['matdate_dt'] = pd.to_datetime(df['MATDATE'], errors='coerce')
                    df = df.dropna(subset=['matdate_dt'])
                    df = df[(df['matdate_dt'] >= min_maturity) & (df['matdate_dt'] <= max_maturity)]
                    print(f"✅ После фильтра по сроку погашения: {len(df)} облигаций")

                # Фильтр по дате размещения
                if 'ISSUEDATE' in df.columns and placement_date:
                    placement_dt = datetime.strptime(placement_date + '-01', '%Y-%m-%d')
                    df['issuedate_dt'] = pd.to_datetime(df['ISSUEDATE'], errors='coerce')
                    df = df[(df['issuedate_dt'] >= placement_dt - timedelta(days=180)) &
                            (df['issuedate_dt'] <= placement_dt + timedelta(days=180))]
                    print(f"✅ После фильтра по дате размещения: {len(df)} облигаций")

                # Фильтр по объёму
                if 'ISSUESIZE' in df.columns and min_volume:
                    df['ISSUESIZE'] = pd.to_numeric(df['ISSUESIZE'], errors='coerce')
                    df = df[df['ISSUESIZE'] >= min_volume]
                    print(f"✅ После фильтра по объёму (≥{min_volume:,.0f} ₽): {len(df)} облигаций")

                if df.empty:
                    print("⚠️ Нет облигаций после фильтрации")
                    return pd.DataFrame()

                # Формируем результат
                comparable = df.head(max_results).copy()

                result = pd.DataFrame()
                result['secid'] = comparable['SECID']
                result['name'] = comparable.get('SHORTNAME', comparable.get('SECNAME', ''))
                result['issuer'] = comparable.get('EMITENT_TITLE', 'Unknown')
                result['face_value'] = pd.to_numeric(comparable.get('FACEVALUE', 1000), errors='coerce').fillna(1000)
                result['coupon_rate'] = pd.to_numeric(comparable.get('COUPONRATE', 0), errors='coerce').fillna(0)
                result['volume'] = pd.to_numeric(comparable.get('ISSUESIZE', 0), errors='coerce').fillna(0)
                result['maturity_date'] = comparable.get('MATDATE', '')
                result['placement_date'] = comparable.get('ISSUEDATE', '')
                result['maturity_months'] = target_maturity_months
                result['credit_rating'] = 'B'
                result['liquidity_score'] = 0.7

                # YTM из реальных данных или купон
                if 'YIELD' in comparable.columns:
                    result['ytm_primary'] = pd.to_numeric(comparable['YIELD'], errors='coerce')
                    mask = result['ytm_primary'].isna()
                    result.loc[mask, 'ytm_primary'] = result.loc[mask, 'coupon_rate']
                    real_ytm_count = (~result['ytm_primary'].isna()).sum()
                    print(f"📈 YTM получена для {real_ytm_count} облигаций из API")
                else:
                    result['ytm_primary'] = result['coupon_rate']

                # Убираем строки без coupon_rate
                result = result[result['coupon_rate'] > 0]

                print(f"✅ Итого: {len(result)} сопоставимых облигаций")
                return result

            except Exception as e:
                print(f"❌ Ошибка MOEX API: {e}")
                import traceback
                traceback.print_exc()
                return pd.DataFrame()

    def calculate_ytm(self,
                     price: float,
                     face_value: float,
                     coupon_rate: float,
                     years_to_maturity: float,
                     frequency: int = 2) -> float:
        """
        Calculate Yield to Maturity using approximation formula.
        """
        annual_coupon = face_value * (coupon_rate / 100)
        ytm = (annual_coupon + (face_value - price) / years_to_maturity) / \
              ((face_value + price) / 2) * 100
        return ytm


def create_sample_bonds_data() -> pd.DataFrame:
    """
    Create sample bonds data for demonstration.
    """
    bonds_data = [
        {
            "secid": "RU000A0ZZ1Q1",
            "name": "ОФЗ 26230",
            "issuer": "Минфин России",
            "face_value": 1000.0,
            "coupon_rate": 7.70,
            "maturity_date": "2027-03-15",
            "placement_date": "2022-03-10",
            "placement_price": 980.0,
            "ytm_primary": 13.20,
            "volume": 500_000_000_000,
            "credit_rating": "AAA",
        },
        {
            "secid": "RU000A1ZZ2R2",
            "name": "Газпром БО-22",
            "issuer": "ПАО Газпром",
            "face_value": 1000.0,
            "coupon_rate": 15.80,
            "maturity_date": "2027-04-20",
            "placement_date": "2026-01-15",
            "placement_price": 1000.0,
            "ytm_primary": 15.90,
            "volume": 15_000_000_000,
            "credit_rating": "AAA",
        },
        {
            "secid": "RU000A2ZZ3S3",
            "name": "Сбер БО-17",
            "issuer": "ПАО Сбербанк",
            "face_value": 1000.0,
            "coupon_rate": 16.20,
            "maturity_date": "2027-05-10",
            "placement_date": "2026-02-20",
            "placement_price": 998.0,
            "ytm_primary": 16.50,
            "volume": 20_000_000_000,
            "credit_rating": "AAA",
        },
        {
            "secid": "RU000A3ZZ4T4",
            "name": "Дева-Агро БО-01",
            "issuer": "ООО Дева-Агро",
            "face_value": 1000.0,
            "coupon_rate": 17.50,
            "maturity_date": "2027-06-01",
            "placement_date": "2026-03-25",
            "placement_price": 995.0,
            "ytm_primary": 18.20,
            "volume": 300_000_000,
            "credit_rating": "BB",
        },
    ]

    df = pd.DataFrame(bonds_data)
    df['maturity_months'] = [12, 13, 12, 14]
    df['liquidity_score'] = [1.0, 0.95, 0.95, 0.65]
    return df
    

