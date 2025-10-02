# This is a sample Python script.

# Press Maj+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# import sys
# from typing import List
from pyspark_base.demo.jobs.drugs import drugs_gen
from pyspark_base.utils.appconfig import Appconfig
from typing import List
from argparse import ArgumentParser


def main() -> None:

    parser = ArgumentParser(
        description="Script that adds 2 numbers from CMD"
    )
    parser.add_argument("--app", required=True, type=str)
    parser.add_argument("--env", choices=["local", "dev"], required=True, type=str)
    args = parser.parse_args()
    lang = [args.app, args.env]
    print(lang)

    match args.app:
        case "no_unity":
            print("You can become a web developer.")
            config = Appconfig(f"pyspark_base/config_{args.env}.ini")
            drugs_gen(config.get_path_src("drugs"), config.get_path_src("pubmed"),
                      config.get_path_src("clinical_trials"), config.get_path_src("journal_gold"))

        case "unity":
            print("You can become a mobile app developer")
        case _:
            print("The language doesn't matter, what matters is solving problems.")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
