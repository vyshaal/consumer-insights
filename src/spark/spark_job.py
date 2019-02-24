from batch_processor import BatchProcessor
from reviews_schema import settings, mappings
from configparser import ConfigParser


def get_year():
    """
    Read the year from the the configuration file
    :return: year whose reviews needs to be processed
    """
    read_file = open(YEAR_PATH, "r")
    year = str(read_file.read())
    if year == "*":
        return year
    if year is None or year == "" or int(year) > int(FINAL_YEAR):
        year = INITIAL_YEAR
    return year


def update_year(year):
    """
    Update the year to next year in the configuration file
    :param year: current year whose reviews are processed
    """
    if year == "*":
        return
    write_file = open(YEAR_PATH, "w")
    write_file.write(str(int(year) + 1))
    write_file.close()


if __name__ == "__main__":
    """
        Reading the configurations 
    """
    config = ConfigParser()
    config.read('/home/ubuntu/consumer-insights/config.ini')
    INITIAL_YEAR = str(config.get('year', 'initial_year'))
    FINAL_YEAR = str(config.get('year', 'final_year'))
    YEAR_PATH = config.get('year', 'year_path')
    es_master = config.get('elasticsearch', 'master')
    spark_master = config.get('spark', 'master')

    """
        Initialize a batch processor that runs spark job
    """
    batch_processor = BatchProcessor(es_master, spark_master)

    """
        Find the year whose reviews needs to be processed
    """
    current_year = get_year()

    """
        Deletion & creation of indices in the database if you're starting from scratch
    """
    batch_processor.conditional_delete_indices(current_year, settings, mappings)

    """
        Reading the reviews and computing the product analytics
    """
    batch_processor.process_reviews(current_year)

    """
        Persisting products and reviews in the database
    """
    batch_processor.save_products()
    batch_processor.save_reviews()
    batch_processor.stop_spark()

    """
        Updating the year in the configuration file
    """
    update_year(current_year)
    print('Done with the year: ' + current_year)
