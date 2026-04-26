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

        Args:
            asset_type: Type of asset (default: stock)
            board_group: Board group (default: stock_bonds)

        Returns:
            DataFrame with bonds list
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

            # Parse securities
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

        Args:
            issuer_name: Name of the issuer
            isin: ISIN code (optional)

        Returns:
            DataFrame with matching bonds
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

            # Filter by issuer if provided
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

        Args:
            secid: Security ID on MOEX

        Returns:
            Dictionary with bond details
        """
        url = f"{self.base_url}/securities/{secid}.json"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Extract bond details
            bond_info = {}

            # Get security description
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

        Args:
            secid: Security ID
            boardid: Board ID (default: TQCB - Corporate Bonds Board)

        Returns:
            DataFrame with market data
        """
        url = f"{self.base_url}/engines/stock/markets/bonds/boards/{boardid}/securities/{secid}.json"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Extract market data
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

        Args:
            secid: Security ID

        Returns:
            YTM as percentage or None if not available
        """
        market_data = self.get_bond_market_data(secid)

        if not market_data.empty:
            # YTM is usually in 'YIELDDAY' or 'YIELD' column
            for col in ['YIELDDAY', 'YIELD', 'YIELDATBEST']:
                if col in market_data.columns:
                    ytm = market_data[col].iloc[0]
                    if pd.notna(ytm):
                        return float(ytm)

        return None

    def find_comparable_bonds(self,
                             target_maturity_months: int = 12,
                             placement_date: str = "2026-04",
                             max_results: int = 10) -> pd.DataFrame:
        """
        Find bonds comparable to DFA based on maturity and placement date.

        Args:
            target_maturity_months: Target maturity in months
            placement_date: Placement date (YYYY-MM format)
            max_results: Maximum number of results

        Returns:
            DataFrame with comparable bonds
        """
        try:
            # Get list of all corporate bonds
            bonds_list = self.fetch_bonds_list()

            if bonds_list.empty:
                print("⚠️ Пустой список облигаций от MOEX API")
                return pd.DataFrame()

            # Filter for corporate bonds with required fields
            required_cols = ['secid', 'name', 'shortname', 'face_value', 'couponrate']
            available_cols = [col for col in required_cols if col in bonds_list.columns]

            if len(available_cols) < 3:
                print("⚠️ Недостаточно данных в ответе MOEX API")
                return pd.DataFrame()

            # Filter bonds by criteria (simplified - in production would use more sophisticated filtering)
            comparable = bonds_list.head(max_results).copy()

            # Rename columns to match expected format
            column_mapping = {
                'secid': 'secid',
                'name': 'name',
                'shortname': 'name',
                'couponrate': 'coupon_rate',
                'face_value': 'face_value',
                'value': 'volume',
                'issuevalue': 'volume'
            }

            # Apply mapping
            for old_col, new_col in column_mapping.items():
                if old_col in comparable.columns and new_col not in comparable.columns:
                    comparable[new_col] = comparable[old_col]

            # Ensure required columns exist with default values
            if 'ytm_primary' not in comparable.columns:
                comparable['ytm_primary'] = comparable.get('coupon_rate', 17.0)

            if 'credit_rating' not in comparable.columns:
                comparable['credit_rating'] = 'B'

            if 'liquidity_score' not in comparable.columns:
                comparable['liquidity_score'] = 0.7

            if 'maturity_date' not in comparable.columns:
                # Estimate maturity date from target months
                from datetime import datetime, timedelta
                placement_dt = datetime.strptime(placement_date, '%Y-%m')
                maturity_dt = placement_dt + timedelta(days=target_maturity_months * 30)
                comparable['maturity_date'] = maturity_dt.strftime('%Y-%m-%d')

            if 'placement_date' not in comparable.columns:
                comparable['placement_date'] = placement_date + '-01'

            if 'maturity_months' not in comparable.columns:
                comparable['maturity_months'] = target_maturity_months

            # Convert volume to numeric if needed
            if 'volume' in comparable.columns:
                comparable['volume'] = pd.to_numeric(comparable['volume'], errors='coerce').fillna(10_000_000)

            # Select and reorder columns
            result_cols = ['secid', 'name', 'issuer', 'coupon_rate', 'face_value',
                          'maturity_date', 'placement_date', 'ytm_primary', 'volume',
                          'credit_rating', 'liquidity_score', 'maturity_months']

            # Add missing columns
            for col in result_cols:
                if col not in comparable.columns:
                    if col == 'issuer':
                        comparable[col] = comparable.get('name', 'Unknown')
                    elif col == 'face_value':
                        comparable[col] = 1000.0
                    elif col in ['coupon_rate', 'ytm_primary']:
                        comparable[col] = 17.0

            # Return only available columns
            available_result_cols = [col for col in result_cols if col in comparable.columns]
            result = comparable[available_result_cols].head(max_results)

            return result

        except Exception as e:
            print(f"⚠️ Ошибка при поиске облигаций: {e}")
            return pd.DataFrame()

    def calculate_ytm(self,
                     price: float,
                     face_value: float,
                     coupon_rate: float,
                     years_to_maturity: float,
                     frequency: int = 2) -> float:
        """
        Calculate Yield to Maturity using approximation formula.

        Args:
            price: Current bond price
            face_value: Face value of bond
            coupon_rate: Annual coupon rate (%)
            years_to_maturity: Years until maturity
            frequency: Coupon payments per year

        Returns:
            YTM as percentage
        """
        # Approximate YTM formula
        annual_coupon = face_value * (coupon_rate / 100)

        ytm = (annual_coupon + (face_value - price) / years_to_maturity) / \
              ((face_value + price) / 2) * 100

        return ytm


def create_sample_bonds_data() -> pd.DataFrame:
    """
    Create sample bonds data for demonstration.
    In real scenario, this would be fetched from MOEX.

    Returns:
        DataFrame with sample bonds
    """
    # Sample bonds comparable to DFA (17% yield, ~12 months)
    bonds_data = [
        {
            "secid": "МаякБP1",
            "name": "БО-01 ООО 'ЦЕНТР НЕДВИЖИМОСТИ МАЯК' P1",
            "issuer": "ООО 'ЦЕНТР НЕДВИЖИМОСТИ МАЯК'",
            "face_value": 1000.0,
            "coupon_rate": 16.5,
            "maturity_date": "2027-05-15",
            "placement_date": "2026-04-10",
            "placement_price": 998.5,
            "ytm_primary": 16.65,
            "volume": 30_000_000,
            "credit_rating": "B+",
        },
        {
            "secid": "МаякБP2",
            "name": "БО-02 ООО 'ЦЕНТР НЕДВИЖИМОСТИ МАЯК' P2",
            "issuer": "ООО 'ЦЕНТР НЕДВИЖИМОСТИ МАЯК'",
            "face_value": 1000.0,
            "coupon_rate": 17.0,
            "maturity_date": "2027-04-20",
            "placement_date": "2026-04-15",
            "placement_price": 1000.0,
            "ytm_primary": 17.0,
            "volume": 40_000_000,
            "credit_rating": "B+",
        },
        {
            "secid": "RUСтрA1",
            "name": "БО-001 РУССТРОЙ АО",
            "issuer": "АО РУССТРОЙ",
            "face_value": 1000.0,
            "coupon_rate": 18.0,
            "maturity_date": "2027-06-01",
            "placement_date": "2026-04-20",
            "placement_price": 995.0,
            "ytm_primary": 18.5,
            "volume": 50_000_000,
            "credit_rating": "B",
        },
        {
            "secid": "UralDev1",
            "name": "БО-001 УРАЛДЕВЕЛОПМЕНТ",
            "issuer": "ООО УРАЛДЕВЕЛОПМЕНТ",
            "face_value": 1000.0,
            "coupon_rate": 16.0,
            "maturity_date": "2027-04-01",
            "placement_date": "2026-03-25",
            "placement_price": 1001.0,
            "ytm_primary": 15.9,
            "volume": 35_000_000,
            "credit_rating": "BB-",
        },
    ]

    df = pd.DataFrame(bonds_data)
    df['maturity_months'] = 12  # All ~12 months
    df['liquidity_score'] = [0.7, 0.8, 0.6, 0.75]  # 0-1 scale

    return df
