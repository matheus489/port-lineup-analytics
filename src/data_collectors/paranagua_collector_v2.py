"""
Enhanced Paranaguá Port Data Collector
Multiple strategies to access the APPA website
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger

from .base_collector import BaseCollector


class ParanaguaCollectorV2(BaseCollector):
    """Enhanced collector for Paranaguá port with multiple access strategies"""

    def __init__(self):
        super().__init__("Paranaguá", "PAR")

        # Try different user agents
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

    def collect_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Collect data using multiple strategies"""
        logger.info(f"Collecting data from Paranaguá from {start_date} to {end_date}")

        strategies = [
            self._strategy_simple_get,
            self._strategy_with_referer,
            self._strategy_simulate_form,
            self._strategy_different_endpoints,
            self._strategy_curl_simulation,
        ]

        for i, strategy in enumerate(strategies, 1):
            logger.info(f"Trying strategy {i}/{len(strategies)}: {strategy.__name__}")
            try:
                df = strategy(start_date, end_date)
                if not df.empty:
                    logger.info(
                        f"✅ Strategy {strategy.__name__} succeeded with {len(df)} records"
                    )
                    return df
            except Exception as e:
                logger.warning(f"❌ Strategy {strategy.__name__} failed: {e}")
                continue

        logger.error("All strategies failed to collect data from Paranaguá")
        return pd.DataFrame()

    def _strategy_simple_get(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Strategy 1: Simple GET request"""
        url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
        response = requests.get(url, verify=False, timeout=30)
        return self._parse_html_content(response.text)

    def _strategy_with_referer(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Strategy 2: With proper referer and headers"""
        headers = {
            "User-Agent": self.user_agents[0],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.appaweb.appa.pr.gov.br/appaweb/",
            "Origin": "https://www.appaweb.appa.pr.gov.br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
        }

        url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        return self._parse_html_content(response.text)

    def _strategy_simulate_form(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Strategy 3: Simulate form submission"""
        # First get the main page
        main_url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx"
        headers = {
            "User-Agent": self.user_agents[1],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }

        main_response = requests.get(
            main_url, headers=headers, verify=False, timeout=30
        )
        soup = BeautifulSoup(main_response.content, "html.parser")

        # Look for any form or hidden inputs
        form_data = {}
        for input_tag in soup.find_all("input"):
            if input_tag.get("type") == "hidden":
                name = input_tag.get("name")
                value = input_tag.get("value", "")
                if name:
                    form_data[name] = value

        # Try POST request
        post_url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx"
        form_data["WCI"] = "relLineUpRetroativo"

        response = requests.post(
            post_url, data=form_data, headers=headers, verify=False, timeout=30
        )
        return self._parse_html_content(response.text)

    def _strategy_different_endpoints(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Strategy 4: Try different URL endpoints"""
        endpoints = [
            "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo",
            "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUp",
            "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=LineUp",
            "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relatorio",
            "https://www.appaweb.appa.pr.gov.br/appaweb/relLineUpRetroativo.aspx",
            "https://www.appaweb.appa.pr.gov.br/appaweb/LineUp.aspx",
        ]

        headers = {
            "User-Agent": self.user_agents[2],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }

        for endpoint in endpoints:
            try:
                logger.info(f"Trying endpoint: {endpoint}")
                response = requests.get(
                    endpoint, headers=headers, verify=False, timeout=30
                )
                df = self._parse_html_content(response.text)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"Endpoint {endpoint} failed: {e}")
                continue

        return pd.DataFrame()

    def _strategy_curl_simulation(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """Strategy 5: Simulate curl request"""
        headers = {"User-Agent": "curl/7.68.0", "Accept": "*/*"}

        url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        return self._parse_html_content(response.text)

    def _parse_html_content(self, html_content: str) -> pd.DataFrame:
        """Parse HTML content to extract ship data"""
        if (
            "Erro de Tempo de Execução" in html_content
            or "Tela não Implementada" in html_content
        ):
            logger.warning("Received error page")
            return pd.DataFrame()

        soup = BeautifulSoup(html_content, "html.parser")

        # Look for tables with ship data
        tables = soup.find_all("table")

        for table in tables:
            # Check if this table contains ship data
            table_text = table.get_text().lower()
            if any(
                keyword in table_text
                for keyword in [
                    "atracados",
                    "programação",
                    "embarcação",
                    "navio",
                    "shamal",
                    "palena",
                ]
            ):
                logger.info("Found potential ship data table")

                # Extract data from this table
                rows = table.find_all("tr")
                data = []

                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if len(cells) >= 5:  # Minimum expected columns
                        row_data = [cell.get_text(strip=True) for cell in cells]

                        # Check if this looks like ship data
                        if self._is_ship_data_row(row_data):
                            data.append(row_data)

                if data:
                    logger.info(f"Found {len(data)} ship data rows")
                    df = pd.DataFrame(data)
                    return self._map_paranagua_columns(df)

        logger.warning("No ship data found in HTML content")
        return pd.DataFrame()

    def _is_ship_data_row(self, row_data: list) -> bool:
        """Check if a row contains ship data"""
        if not row_data or len(row_data) < 5:
            return False

        first_cell = row_data[0].lower()

        # Look for ship indicators
        ship_indicators = [
            "programação",
            "77505",
            "77306",
            "77425",
            "77503",
            "77389",
            "77545",
            "77469",
        ]

        # Check if first cell looks like a programming number or ship name
        if any(indicator in first_cell for indicator in ship_indicators):
            return True

        # Check if we have ship-like data (IMO numbers, ship names, etc.)
        if len(row_data) >= 10:  # Full ship data row
            # Look for IMO numbers (usually 7 digits)
            for cell in row_data:
                if cell.isdigit() and len(cell) == 7:
                    return True

        return False

    def _map_paranagua_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map Paranaguá columns to standard format"""
        # Based on the actual table structure from the image
        # The columns are: Programação | DUV | Berço | Embarcação | IMO | LOA | DWT | Bordo | Sentido | Agência | Operador | Mercadoria | Atracação | Chegada | Janela Operacional | Prancha | Tons/Dia | Previsto | Realizado | Saldo Operador | Saldo Total
        column_mapping = {
            0: "programacao",  # Programação
            1: "duv",  # DUV
            2: "berco",  # Berço
            3: "navio",  # Embarcação
            4: "imo",  # IMO
            5: "loa",  # LOA
            6: "dwt",  # DWT
            7: "bordo",  # Bordo
            8: "sentido",  # Sentido
            9: "agencia",  # Agência
            10: "operador",  # Operador
            11: "produto",  # Mercadoria
            12: "atracacao",  # Atracação
            13: "data_chegada",  # Chegada
            14: "janela_operacional",  # Janela Operacional
            15: "prancha_capacidade",  # Prancha (t/dia)
            16: "tons_dia",  # Tons/Dia
            17: "volume_previsto",  # Previsto
            18: "volume_realizado",  # Realizado
            19: "saldo_operador",  # Saldo Operador
            20: "saldo_total",  # Saldo Total
        }

        # Rename columns by index
        df = df.rename(
            columns={
                i: column_mapping.get(i, f"col_{i}") for i in range(len(df.columns))
            }
        )

        # Add standard columns
        df["porto"] = "PARANAGUÁ"
        df["porto_codigo"] = "PAR"
        df["data_coleta"] = datetime.now()
        df["fonte"] = "ParanaguaCollectorV2"

        # Map volume from the correct field
        if "volume_previsto" in df.columns:
            df["volume"] = df["volume_previsto"]

        # Clean and standardize data
        df = self.standardize_data(df)

        return df
