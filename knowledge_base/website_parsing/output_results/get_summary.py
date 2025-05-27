from idlelib.outwin import file_line_pats

from knowledge_base.website_parsing.website_services.reports import summarize


def get_report_from_parse_file(file_path: str):
    """Отчет по обработке файла с парсингом"""
    summary_data = summarize(file_path)
    print("\n--- Отчёт ---")
    for key, value in summary_data.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    file_with_parsed_data = "academydpo_parsed_data_2025_05_23_00-13.json"
    get_report_from_parse_file(file_with_parsed_data)