# This is a sample Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# import sys
# from typing import List
from pyspark_base.demo.jobs.drugs import drugs_gen
from pyspark_base.utils.appconfig import Appconfig


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def main() -> None:
    config = Appconfig("pyspark_base/config.ini")
    print("Unuse DE Main Routine.")
    print_hi('PyCharm')
    drugs_gen(config.get_path_src("drugs"), config.get_path_src("pubmed"), config.get_path_src("clinical_trials"), "str")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
