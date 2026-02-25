import sys
import os

sys.path.insert(0, os.path.dirname(__file__))


def run_full_pipeline():

    # Creating DB tables 
    from db.schema import create_tables
    create_tables()

    # Loading historical Kaggle tracks data
    from pipelines.analysis.load_historical import run_historical_load
    run_historical_load()#

    # Loading Kaggle artists data 
    from pipelines.analysis.load_artists import run_artists_load
    run_artists_load()

    # Fetching from Spotify API
    from pipelines.ingestion.load_to_dw import run_pipeline1
    run_pipeline1()

    # Running analysis pipeline 
    from pipelines.analysis.marketing_analysis import run_analysis
    run_analysis()



def run_api_only():
  
  from pipelines.ingestion.load_to_dw import run_pipeline1
  run_pipeline1()




def run_analysis_only():
  
  from pipelines.analysis.marketing_analysis import run_analysis
  run_analysis()





def run_charts():
    # Generating charts 
    from pipelines.analysis.visualize import run_visualize
    run_visualize()











# Main function
def main():
    print("=== Spotify Data Warehouse ===")
    print("1. Run full pipeline (setup + historical + API + analysis)")
    print("2. Run API ingestion only (daily use)")
    print('3. Run marketing analysis only')
    print("4. Generate charts")
    print("5. Exit")

    choice = input("\nEnter choice (1-5): ").strip()

    if choice == '1':
        run_full_pipeline()
    elif choice == '2':
        run_api_only()
    elif choice == '3':
        run_analysis_only()
    elif choice == '4':
        run_charts()
    elif choice == '5':
        print("Bye")
    else:
        print("Invalid choice")


if __name__ == '__main__':
    main()
