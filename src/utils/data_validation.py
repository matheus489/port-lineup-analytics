"""
Data validation utilities for ship lineup data
"""

from typing import Any, Dict, List, Tuple

import pandas as pd
from loguru import logger

from config import Config


class DataValidator:
    """Validates ship lineup data according to business rules"""

    def __init__(self):
        self.validation_rules = Config.VALIDATION_RULES

    def validate_dataframe(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate entire DataFrame and return cleaned data with validation report

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (cleaned_dataframe, validation_report)
        """
        logger.info(f"Starting validation of {len(df)} records")

        validation_report = {
            "total_records": len(df),
            "valid_records": 0,
            "invalid_records": 0,
            "validation_errors": [],
            "data_quality_score": 0.0,
        }

        # Start with original data
        cleaned_df = df.copy()

        # Run all validation checks
        cleaned_df, errors = self._validate_required_columns(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        cleaned_df, errors = self._validate_port_names(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        cleaned_df, errors = self._validate_directions(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        cleaned_df, errors = self._validate_volumes(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        cleaned_df, errors = self._validate_dates(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        cleaned_df, errors = self._validate_ship_names(cleaned_df)
        validation_report["validation_errors"].extend(errors)

        # Calculate final metrics
        validation_report["valid_records"] = len(cleaned_df)
        validation_report["invalid_records"] = (
            validation_report["total_records"] - validation_report["valid_records"]
        )

        if validation_report["total_records"] > 0:
            validation_report["data_quality_score"] = (
                validation_report["valid_records"] / validation_report["total_records"]
            ) * 100

        logger.info(
            f"Validation completed: {validation_report['valid_records']}/{validation_report['total_records']} valid records"
        )
        logger.info(
            f"Data quality score: {validation_report['data_quality_score']:.2f}%"
        )

        return cleaned_df, validation_report

    def _validate_required_columns(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[str]]:
        """Validate that required columns exist and have data"""
        errors = []
        required_columns = self.validation_rules["required_columns"]

        # Check if required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
            return df, errors

        # Check for null values in required columns
        for col in required_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                errors.append(f"Column '{col}' has {null_count} null values")
                # Remove rows with null values in required columns
                df = df.dropna(subset=[col])

        return df, errors

    def _validate_port_names(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate port names"""
        errors = []
        valid_ports = self.validation_rules["valid_ports"]

        if "porto" in df.columns:
            invalid_ports = df[~df["porto"].isin(valid_ports)]["porto"].unique()
            if len(invalid_ports) > 0:
                errors.append(f"Invalid port names found: {list(invalid_ports)}")
                # Remove rows with invalid ports
                df = df[df["porto"].isin(valid_ports)]

        return df, errors

    def _validate_directions(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate direction values"""
        errors = []
        valid_directions = self.validation_rules["valid_directions"]

        if "sentido" in df.columns:
            invalid_directions = df[~df["sentido"].isin(valid_directions)][
                "sentido"
            ].unique()
            if len(invalid_directions) > 0:
                errors.append(f"Invalid directions found: {list(invalid_directions)}")
                # Remove rows with invalid directions
                df = df[df["sentido"].isin(valid_directions)]

        return df, errors

    def _validate_volumes(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate volume values"""
        errors = []

        if "volume" in df.columns:
            min_volume = self.validation_rules["min_volume"]
            max_volume = self.validation_rules["max_volume"]

            # Convert to numeric
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

            # Check for invalid volumes
            invalid_volumes = df[
                (df["volume"] < min_volume)
                | (df["volume"] > max_volume)
                | df["volume"].isnull()
            ]

            if len(invalid_volumes) > 0:
                errors.append(f"Invalid volumes found: {len(invalid_volumes)} records")
                # Remove rows with invalid volumes
                df = df[
                    (df["volume"] >= min_volume)
                    & (df["volume"] <= max_volume)
                    & df["volume"].notnull()
                ]

        return df, errors

    def _validate_dates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate date fields"""
        errors = []
        date_columns = ["data_chegada", "data_partida"]

        for col in date_columns:
            if col in df.columns:
                # Convert to datetime
                df[col] = pd.to_datetime(df[col], errors="coerce")

                # Check for invalid dates
                invalid_dates = df[df[col].isnull()]
                if len(invalid_dates) > 0:
                    errors.append(
                        f"Invalid dates in column '{col}': {len(invalid_dates)} records"
                    )
                    # Remove rows with invalid dates
                    df = df[df[col].notnull()]

                # Check for future dates (more than 1 year in the future)
                future_threshold = pd.Timestamp.now() + pd.Timedelta(days=365)
                future_dates = df[df[col] > future_threshold]
                if len(future_dates) > 0:
                    errors.append(
                        f"Future dates in column '{col}': {len(future_dates)} records"
                    )

        return df, errors

    def _validate_ship_names(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Validate ship names"""
        errors = []

        if "navio" in df.columns:
            # Check for empty or very short ship names
            invalid_names = df[
                (df["navio"].str.len() < 2)
                | (df["navio"].isnull())
                | (df["navio"].str.strip() == "")
            ]

            if len(invalid_names) > 0:
                errors.append(f"Invalid ship names: {len(invalid_names)} records")
                # Remove rows with invalid ship names
                df = df[
                    (df["navio"].str.len() >= 2)
                    & (df["navio"].notnull())
                    & (df["navio"].str.strip() != "")
                ]

        return df, errors

    def generate_validation_report(self, validation_report: Dict[str, Any]) -> str:
        """Generate a formatted validation report"""
        report = f"""
Data Validation Report
=====================
Total Records: {validation_report['total_records']}
Valid Records: {validation_report['valid_records']}
Invalid Records: {validation_report['invalid_records']}
Data Quality Score: {validation_report['data_quality_score']:.2f}%

Validation Errors:
"""

        if validation_report["validation_errors"]:
            for error in validation_report["validation_errors"]:
                report += f"- {error}\n"
        else:
            report += "- No validation errors found\n"

        return report

    def validate_incremental_data(
        self, new_df: pd.DataFrame, existing_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate incremental data against existing data to detect duplicates and inconsistencies

        Args:
            new_df: New data to validate
            existing_df: Existing data for comparison

        Returns:
            Tuple of (validated_new_data, validation_report)
        """
        logger.info(
            f"Validating incremental data: {len(new_df)} new records against {len(existing_df)} existing records"
        )

        validation_report = {
            "new_records": len(new_df),
            "duplicate_records": 0,
            "inconsistent_records": 0,
            "valid_new_records": 0,
            "validation_errors": [],
        }

        # Check for duplicates based on key fields
        if not existing_df.empty and not new_df.empty:
            key_columns = ["porto", "navio", "data_chegada", "produto"]
            existing_keys = existing_df[key_columns].drop_duplicates()
            new_keys = new_df[key_columns].drop_duplicates()

            # Find duplicates
            duplicates = pd.merge(new_keys, existing_keys, on=key_columns, how="inner")
            if len(duplicates) > 0:
                validation_report["duplicate_records"] = len(duplicates)
                validation_report["validation_errors"].append(
                    f"Found {len(duplicates)} duplicate records"
                )

                # Remove duplicates from new data
                new_df = new_df.drop_duplicates(subset=key_columns, keep="first")

        # Validate the remaining new data
        validated_df, standard_report = self.validate_dataframe(new_df)
        validation_report["valid_new_records"] = len(validated_df)
        validation_report["validation_errors"].extend(
            standard_report["validation_errors"]
        )

        logger.info(
            f"Incremental validation completed: {validation_report['valid_new_records']} valid new records"
        )

        return validated_df, validation_report
