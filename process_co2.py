import clean_co2 as cleanco2
import summary_co2 as summaryco2

def main():
    # firsr put new data in "combined" folder
    cleanco2.clean()
    cleanco2.reformat()
    summaryco2.summary_label()
    summaryco2.summary_stat()
    summaryco2.summary_all_general()
    summaryco2.remove_dup_files()
    # final summary is in combined/reformat/summary

main()
