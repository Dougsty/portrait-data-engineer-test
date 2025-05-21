from pipeline.raw import load_all_raw_data
from pipeline.trusted import transform_all_data
from pipeline.refined import refine_all_data


def main():
    """
    Main function to run the ETL pipeline.
    """
    # Load raw data
    print("Loading raw data...")
    load_all_raw_data()

    # Load trusted data
    print("Loading trusted data...")
    transform_all_data()

    # Load refined data
    print("Loading refined data...")
    refine_all_data()


if __name__ == "__main__":
    main()
