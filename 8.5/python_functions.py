# python_functions.py
import requests
import pandas as pd
import json
import jsonschema
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


# API_CALL функции
def call_api(api_url: str, method: str = "GET", headers: Dict = None,
             timeout: int = 30, output_path: str = None, **kwargs) -> Dict:
    """
    Вызов API endpoint
    """
    try:
        response = requests.request(
            method=method,
            url=api_url,
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()

        data = response.json()

        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"API call failed: {e}")
        raise


# VALIDATION функции
def validate_json_data(data_path: str, rules: List[Dict], **kwargs) -> Dict:
    """
    Валидация JSON данных по правилам
    """
    with open(data_path, 'r') as f:
        data = json.load(f)

    validation_results = {
        "valid": True,
        "errors": [],
        "passed_rules": 0,
        "failed_rules": 0
    }

    for rule in rules:
        rule_type = rule.get("rule_type")

        if rule_type == "not_null":
            result = _validate_not_null(data, rule.get("fields", []))
        elif rule_type == "value_range":
            result = _validate_value_range(data, rule)
        elif rule_type == "string_format":
            result = _validate_string_format(data, rule)
        elif rule_type == "unique_values":
            result = _validate_unique_values(data, rule)
        else:
            result = {"valid": False, "error": f"Unknown rule type: {rule_type}"}

        if result["valid"]:
            validation_results["passed_rules"] += 1
        else:
            validation_results["valid"] = False
            validation_results["failed_rules"] += 1
            validation_results["errors"].append(result["error"])

    return validation_results


def _validate_not_null(data: Dict, fields: List[str]) -> Dict:
    """Проверка на null значения"""
    errors = []
    for field in fields:
        if field not in data or data[field] is None:
            errors.append(f"Field '{field}' is null or missing")

    return {
        "valid": len(errors) == 0,
        "error": "; ".join(errors) if errors else None
    }


def _validate_value_range(data: Dict, rule: Dict) -> Dict:
    """Проверка диапазона значений"""
    field = rule.get("field")
    min_val = rule.get("min")
    max_val = rule.get("max")

    if field not in data:
        return {"valid": False, "error": f"Field '{field}' not found"}

    value = data[field]
    if (min_val is not None and value < min_val) or (max_val is not None and value > max_val):
        return {
            "valid": False,
            "error": f"Field '{field}' value {value} not in range [{min_val}, {max_val}]"
        }

    return {"valid": True}


def _validate_string_format(data: Dict, rule: Dict) -> Dict:
    """Проверка формата строки"""
    import re

    field = rule.get("field")
    pattern = rule.get("pattern")

    if field not in data:
        return {"valid": False, "error": f"Field '{field}' not found"}

    if not re.match(pattern, str(data[field])):
        return {
            "valid": False,
            "error": f"Field '{field}' value does not match pattern: {pattern}"
        }

    return {"valid": True}


def _validate_unique_values(data: Dict, rule: Dict) -> Dict:
    """Проверка уникальности значений"""
    # Для массива данных
    if isinstance(data, list):
        field = rule.get("field")
        values = [item[field] for item in data if field in item]
        if len(values) != len(set(values)):
            return {
                "valid": False,
                "error": f"Field '{field}' contains duplicate values"
            }
    return {"valid": True}
