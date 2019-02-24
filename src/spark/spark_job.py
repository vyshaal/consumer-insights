from batch_processor import BatchProcessor
from reviews_schema import settings, mappings
from configparser import ConfigParser


def get_year():
    read_file = open(YEAR_PATH, "r")
    year = str(read_file.read())
    if year == "*":
        return year
    if year is None or year == "" or int(year) > int(FINAL_YEAR):
        year = INITIAL_YEAR
    return year


def update_year(year):
    if year == "*":
        return
    write_file = open(YEAR_PATH, "w")
    write_file.write(str(int(year) + 1))
    write_file.close()


if __name__ == "__main__":
    config = ConfigParser()
    config.read('/home/ubuntu/consumer-insights/config.ini')
    INITIAL_YEAR = str(config.get('year', 'initial_year'))
    FINAL_YEAR = str(config.get('year', 'final_year'))
    YEAR_PATH = config.get('year', 'year_path')
    ELASTICSEARCH_MASTER = config.get('elasticsearch', 'master')
    batch_processor = BatchProcessor(ELASTICSEARCH_MASTER)
    current_year = get_year()
    batch_processor.conditional_delete_indices(current_year, settings, mappings)
    batch_processor.process_reviews(current_year)
    batch_processor.save_products()
    batch_processor.save_reviews()
    batch_processor.stop_spark()
    update_year(current_year)
    print('Done with the year: ' + current_year)
