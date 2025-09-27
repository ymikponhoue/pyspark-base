# This is a sample Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# import sys
# from typing import List
from pyspark_base.demo.jobs.drugs import drugs_gen


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def main() -> None:
    """The main routine."""
    print("Unuse DE Main Routine.")
    print_hi('PyCharm')
    drugs_gen("data/drugs.csv", "data/pubmed.csv", "data/clinical_trials.csv", "str")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
