"""
Base collector class for ship lineup data
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from loguru import logger

from config import Config


class BaseCollector(ABC):
    """Abstract base class for data collectors"""

    def __init__(self, port_name: str, port_code: str):
        self.port_name = port_name
        self.port_code = port_code
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Cache-Control": "max-age=0",
            }
        )
        # Disable SSL verification for problematic sites
        self.session.verify = False
        # Disable SSL warnings
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @abstractmethod
    def collect_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Collect data from the source

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with collected data
        """
        pass

    def make_request(
        self, url: str, params: Optional[Dict] = None
    ) -> requests.Response:
        """
        Make HTTP request with retry logic

        Args:
            url: URL to request
            params: Query parameters

        Returns:
            Response object
        """
        for attempt in range(Config.MAX_RETRIES):
            try:
                response = self.session.get(
                    url, params=params, timeout=Config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt == Config.MAX_RETRIES - 1:
                    raise
                continue

    def standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize collected data format

        Args:
            df: Raw DataFrame

        Returns:
            Standardized DataFrame
        """
        # Add metadata columns
        df["porto"] = self.port_name
        df["porto_codigo"] = self.port_code
        df["data_coleta"] = pd.Timestamp.now()
        df["fonte"] = self.__class__.__name__

        # Ensure required columns exist (but don't override existing ones)
        required_columns = Config.VALIDATION_RULES["required_columns"]
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        return df

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate collected data

        Args:
            df: DataFrame to validate

        Returns:
            Validated DataFrame
        """
        if df.empty:
            logger.info(f"No data to validate for {self.port_name}")
            return df

        rules = Config.VALIDATION_RULES
        initial_count = len(df)

        # Remove rows with missing essential data (be more flexible)
        essential_columns = ["navio"]
        df = df.dropna(subset=essential_columns)

        # Only validate port names if porto column exists
        if "porto" in df.columns:
            valid_ports = rules["valid_ports"]
            df = df[df["porto"].isin(valid_ports)]

        # Only validate directions if sentido column exists
        if "sentido" in df.columns:
            valid_directions = rules["valid_directions"]
            df = df[df["sentido"].isin(valid_directions)]

        # Validate volume if it exists
        if "volume" in df.columns:
            df = df[
                (df["volume"] >= rules["min_volume"])
                & (df["volume"] <= rules["max_volume"])
            ]

        final_count = len(df)
        logger.info(
            f"Validated {final_count}/{initial_count} records for {self.port_name}"
        )
        return df
