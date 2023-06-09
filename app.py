import csv
from functools import reduce
from multiprocessing import Pool


def count_words(names: list[str]) -> dict:
    count_names = {}
    for name in names:
        if not name in count_names:
            count_names[name] = 0
        count_names[name] += 1
    return count_names


def calculate(dict1: dict[str, int], dict2: dict[str, int]) -> dict:
    combined = {}

    for key in dict1:
        combined[key] = dict1[key]

    for key in dict2:
        if key in combined:
            combined[key] += dict2[key]
        else:
            combined[key] = dict2[key]

    return combined


if __name__ == "__main__":
    all_results: list[dict] = []
    with open('baby-names-state.csv') as file:
        reader = csv.DictReader(file)
        batches: list[list[str]] = []
        names: list[str] = []
        for i, row in enumerate(reader):
            name: str = str(row['name'])
            state: str = str(row['state_abb'])
            if state != 'CA':
                continue

            names.append(name)

            if len(names) == 100:
                batches.append(names)
                names = []

            if len(batches) == 100:
                with Pool() as pool:
                    results: list[dict] = pool.map(count_words, batches)
                    result: dict = reduce(calculate, results)
                all_results.append(result)
                batches = []
        total = reduce(calculate, all_results)

        name = ""
        count = 0
        for key, val in total.items():
            if val > count:
                count = val
                name = key
        print(name, count)
