import csv
import json
import os
import pandas as pd

from typing import List, Iterable, Optional
from presidio_analyzer import BatchAnalyzerEngine, DictAnalyzerResult
from presidio_anonymizer import BatchAnonymizerEngine

class CSVAnalyzer(BatchAnalyzerEngine):
    """_summary_

    Args:
        BatchAnalyzerEngine (_type_): _description_
    """
    def analyze_csv(
        self,
        csv_full_path: str,
        language: str,
        keys_to_skip: Optional[List[str]] = None,
        **kwargs,
    ) -> Iterable[DictAnalyzerResult]:

        with open(csv_full_path, 'r') as csv_file:
            csv_list = list(csv.reader(csv_file))
            csv_dict = {header: list(map(str, values)) for header, *values in zip(*csv_list)}
            analyzer_results = self.analyze_dict(csv_dict, language, keys_to_skip)
            return list(analyzer_results)

def analyze_csv_files(issues_csv_path: str, comments_csv_path: str) -> List[DictAnalyzerResult]:
    """_summary_

    Args:
        issues_csv_path (str): _description_
        comments_csv_path (str): _description_

    Returns:
        List[DictAnalyzerResult]: _description_
    """
    analyzer = CSVAnalyzer()
    issues_results = analyzer.analyze_csv(issues_csv_path, language="en")
    comments_results = analyzer.analyze_csv(comments_csv_path, language="en")
    return issues_results, comments_results

def anonymize_csv_files(analyzer_results: List[DictAnalyzerResult]) -> List[Dict]:
    """_summary_

    Args:
        analyzer_results (List[DictAnalyzerResult]): _description_

    Returns:
        List[Dict]: _description_
    """
    anonymizer = BatchAnonymizerEngine()
    anonymized_results = anonymizer.anonymize_dict(analyzer_results)
    return anonymized_results

if not os.path.exists('out/files'):
    os.makedirs('out/files')

issues_csv_path = 'in/tables/issues.csv'
comments_csv_path = 'in/tables/comments.csv'

issues_results, comments_results = analyze_csv_files(issues_csv_path, comments_csv_path)

anonymized_issues_results = anonymize_csv_files(issues_results)
anonymized_comments_results = anonymize_csv_files(comments_results)

issues_output_csv_path = 'out/files/issues_anonymized.csv'
comments_output_csv_path = 'out/files/comments_anonymized.csv'

for result, output_csv_path in zip((anonymized_issues_results, anonymized_comments_results),
                                   (issues_output_csv_path, comments_output_csv_path)):
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=result[0].keys())
        writer.writeheader()
        writer.writerows(result)

print("Anonymized CSV files saved successfully.")
