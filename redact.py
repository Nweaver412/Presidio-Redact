import csv
import pprint
from typing import List, Iterable, Optional

from presidio_analyzer import BatchAnalyzerEngine, DictAnalyzerResult
from presidio_anonymizer import BatchAnonymizerEngine
class CSVAnalyzer(BatchAnalyzerEngine):

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

# No __main__ block, just the class definition and method
def dict_to_csv(data, filename):
    # Extract keys and transpose values
    keys = list(data.keys())
    values = list(zip(*data.values()))
    
    # Write data to CSV file
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(keys)  # Write header
        writer.writerows(values)


# Instantiate and use the CSVAnalyzer class
analyzer = CSVAnalyzer()
analyzer_results = analyzer.analyze_csv('comments.csv', language="en")
# pprint.pprint(analyzer_results)

anonymizer = BatchAnonymizerEngine()
anonymized_results = anonymizer.anonymize_dict(analyzer_results)
print(anonymized_results)
dict_to_csv(anonymized_results, 'comments_anonymized.csv')

