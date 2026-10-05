import zipfile
import gzip
import io
import os

ZIP_PATH = "data/stocks_1m_csvs.zip"

with zipfile.ZipFile(ZIP_PATH, "r") as z:

    files = z.namelist()

    print("\n===== DATASET INSPECTION =====")
    print("Total stock files:", len(files))

    print("\nFirst 20 files:")
    for f in files[:20]:
        print(f)

    # Pick the first .csv.gz file
    sample_file = next(
        f for f in files
        if f.endswith(".csv.gz")
    )

    print("\n===== SAMPLE FILE =====")
    print("File:", sample_file)

    # Read compressed CSV correctly
    with z.open(sample_file) as compressed_file:
        with gzip.GzipFile(fileobj=compressed_file) as gz:
            with io.TextIOWrapper(
                gz,
                encoding="utf-8",
                errors="replace"
            ) as text_file:

                print("\nFirst 10 lines:\n")

                for i in range(10):
                    line = text_file.readline()

                    if not line:
                        break

                    print(line.rstrip())

print("\n===== DONE =====")